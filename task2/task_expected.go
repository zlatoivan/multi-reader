package main

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
)

// SizedReadSeekCloser - интерфейс ридера с возможностью seek и знанием своего размера.
// Мы будем объединять несколько таких ридеров в один длинный поток.
type SizedReadSeekCloser interface {
	io.ReadSeekCloser
	Size() int64
}

const (
	// bufferSize — размер одного блока префетча.
	// Это «порция» чтения префетчером; влияет на частоту I/O.
	bufferSize = 1024 * 1024
	// defaultBuffersNum — количество блоков в окне буфера.
	// Итоговая ёмкость буфера: defaultBuffersNum * bufferSize.
	defaultBuffersNum = 4
	// prefetchChunkLimit — верхняя граница размера одного чанка, чтобы окно не было чрезмерно большим
	// при маленьких источниках и тестовых сценариях. Это не меняет семантику, только гранулярность.
	prefetchChunkLimit = 25
)

// MultiReader объединяет несколько SizedReadSeekCloser в единый конкатенированный поток
// и поддерживает асинхронный префетч с ограниченным буфером.
// Внешний Read отдаёт байты только из внутреннего буфера, который наполняется фоном.
type MultiReader struct {
	readers     []SizedReadSeekCloser // исходные ридеры
	totalSize   int64                 // суммарный размер
	prefixSizes []int64               // абсолютная стартовая позиция i-го ридера

	// Текущая логическая позиция чтения (для пользователя)
	absPos int64

	// Кольцевое окно из чанков фиксированного размера
	bufferStart    int64 // абсолютная позиция начала окна
	chunks         []chunk
	head           int
	tail           int
	count          int
	consumedInHead int // потреблено байт в головном чанке
	buffersNum     int

	// Состояние
	closed      bool
	prefetchErr error // последняя ошибка источника (включая EOF)

	// Синхронизация и управление жизненным циклом префетчера
	cond             *sync.Cond // условная переменная для координации читателя и продюсера; используем cond.L как единственный локер
	prefetchStarted  bool
	prefetchStopping bool
	prefetchDone     chan struct{} // канал для ожидания завершения горутины префетчера

	// Параметры запуска/перезапуска префетча
	pfPos      int64
	pfNeedSeek bool // префетчер должен сделать Seek на pfPos перед чтением
}

type chunk struct {
	buf []byte // срез байт, куда префетчер прочитал данные
	n   int    // фактическое число полезных байт в buf (может быть меньше len(buf), т.к. буфер часто выделяется с запасом)
}

// Проверка, что MultiReader удовлетворяет интерфейсу SizedReadSeekCloser
var _ SizedReadSeekCloser = (*MultiReader)(nil)

// NewMultiReader создаёт конкатенированный ридер поверх набора SizedReadSeekCloser
// с поддержкой асинхронного префетча. Префетчер запускается лениво при первом чтении.
func NewMultiReader(buffersNum int, readers ...SizedReadSeekCloser) *MultiReader {
	// нормализуем buffersNum
	if buffersNum <= 0 {
		buffersNum = defaultBuffersNum
	}

	// Считаем префиксные суммы и общий размер заранее — понадобится для быстрого поиска активного ридера
	prefixSizes := make([]int64, len(readers)+1)
	var total int64
	for i, r := range readers {
		prefixSizes[i] = total
		total += r.Size()
	}
	prefixSizes[len(readers)] = total

	return &MultiReader{
		readers:          readers,
		totalSize:        total,
		prefixSizes:      prefixSizes,
		absPos:           0,
		bufferStart:      0,
		chunks:           make([]chunk, buffersNum), // кольцевой буфер чанков фиксированной длины
		head:             0,
		tail:             0,
		count:            0,
		consumedInHead:   0,
		buffersNum:       buffersNum,
		closed:           false,
		prefetchErr:      nil,
		cond:             sync.NewCond(&sync.Mutex{}),
		prefetchStarted:  false,
		prefetchStopping: false,
		prefetchDone:     nil,
		pfPos:            0,
		pfNeedSeek:       true,
	}
}

// Size возвращает суммарный размер всех ридеров.
func (m *MultiReader) Size() int64 {
	return m.totalSize
}

// Read читает данные из внутреннего буфера (очереди чанков).
func (m *MultiReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	if m.closed {
		return 0, io.ErrClosedPipe
	}
	// Если уже на EOF, не запускаем префетчер зря
	if m.absPos == m.totalSize {
		return 0, io.EOF
	}
	// Ленивая инициализация префетчера — до первого реального чтения
	if !m.prefetchStarted {
		m.startPrefetchLocked()
	}

	n := 0
	for n < len(p) {
		// Достигнут общий EOF — больше данных не будет
		if m.absPos == m.totalSize {
			return n, nil
		}

		// Данных нет либо голова исчерпана — ждём пополнения/ошибки
		if m.count == 0 || (m.count > 0 && m.consumedInHead >= m.chunks[m.head].n) {
			if m.prefetchErr != nil {
				if n > 0 {
					return n, nil
				}
				return 0, m.prefetchErr
			}
			m.cond.Wait()
			continue
		}

		// Копируем из головного чанка и сдвигаем окно
		ch := &m.chunks[m.head]
		available := ch.n - m.consumedInHead
		need := len(p) - n
		toCopy := available
		if toCopy > need {
			toCopy = need
		}
		copy(p[n:n+toCopy], ch.buf[m.consumedInHead:m.consumedInHead+toCopy])
		n += toCopy
		m.absPos += int64(toCopy)
		m.consumedInHead += toCopy

		// Если головной чанк закончился — освобождаем слот и сдвигаем head
		if m.consumedInHead == ch.n {
			m.bufferStart += int64(ch.n)
			m.head = (m.head + 1) % m.buffersNum
			m.count--
			m.consumedInHead = 0
			m.chunks[(m.head+m.buffersNum-1)%m.buffersNum] = chunk{}
			m.cond.Broadcast() // разбудим префетчер, если он ждал свободного слота
		}
	}

	return n, nil
}

// Seek перемещает курсор. Если позиция внутри текущего окна — переиндексируем чанк/смещение.
// Иначе очищаем окно и перезапускаем префетч с новой позиции.
func (m *MultiReader) Seek(offset int64, whence int) (int64, error) {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	if m.closed {
		return 0, io.ErrClosedPipe
	}

	var base int64
	switch whence {
	case io.SeekStart:
		base = 0
	case io.SeekCurrent:
		base = m.absPos
	case io.SeekEnd:
		base = m.totalSize
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	seekPos := base + offset
	if seekPos < 0 || seekPos > m.totalSize {
		return 0, fmt.Errorf("seek position (%d) should be >= 0 and <= totalSize (%d)", seekPos, m.totalSize)
	}

	// Позволяем откат ровно на 1 байт внутри головного чанка без сброса
	if m.count > 0 {
		headStart := m.bufferStart
		effectiveStart := headStart + int64(m.consumedInHead)
		if seekPos == effectiveStart-1 && seekPos >= headStart {
			m.consumedInHead = int(seekPos - headStart)
			m.absPos = seekPos
			return seekPos, nil
		}
	}

	// Попадание внутрь эффективного окна (один проход без предварительного суммирования)
	effectiveStart := m.bufferStart + int64(m.consumedInHead)
	seekDelta := int(seekPos - effectiveStart)
	if seekDelta >= 0 && m.count > 0 {
		idx := m.head
		consumedInCurrent := m.consumedInHead
		for i := 0; i < m.count; i++ {
			remain := m.chunks[idx].n - consumedInCurrent
			if seekDelta < remain {
				m.head = idx
				m.consumedInHead = consumedInCurrent + seekDelta
				m.absPos = seekPos
				return seekPos, nil
			}
			seekDelta -= remain
			idx = (idx + 1) % m.buffersNum
			consumedInCurrent = 0
		}
	}

	// Вне окна — сбрасываем очередь и перезапускаем префетч с новой позиции
	for m.count > 0 {
		m.chunks[m.head] = chunk{}
		m.head = (m.head + 1) % m.buffersNum
		m.count--
	}
	m.tail = m.head
	m.consumedInHead = 0
	m.bufferStart = seekPos
	m.prefetchErr = nil
	m.pfPos = seekPos
	m.pfNeedSeek = true
	m.cond.Broadcast()
	m.absPos = seekPos

	return seekPos, nil
}

// Close завершает префетч и закрывает все источники, агрегируя ошибки.
// Сценарий: ставим флаг закрытия, будим префетчер, ждём его завершения, затем закрываем ридеры.
func (m *MultiReader) Close() error {
	m.cond.L.Lock()
	if m.closed {
		m.cond.L.Unlock()
		return nil
	}

	m.closed = true
	if m.prefetchStarted && !m.prefetchStopping {
		m.prefetchStopping = true
		m.cond.Broadcast()
	}
	done := m.prefetchDone
	m.cond.L.Unlock()

	if done != nil {
		<-done
	}

	var multiErr error
	for _, r := range m.readers {
		err := r.Close()
		if err != nil {
			multiErr = errors.Join(err, multiErr)
		}
	}

	if multiErr != nil {
		return fmt.Errorf("error when closing: %w", multiErr)
	}
	return nil
}

// Внутреннее: запуск префетчера. Требует удержания блокировки cond.L.
// Подготавливает начальные параметры и стартует горутину-продюсера.
func (m *MultiReader) startPrefetchLocked() {
	if m.prefetchStarted {
		return
	}
	m.prefetchStarted = true
	m.prefetchDone = make(chan struct{})
	m.pfPos = m.absPos
	m.pfNeedSeek = true
	go m.prefetchLoop()
}

// Внутреннее: горутина префетча. Наполняет очередь чанков.
func (m *MultiReader) prefetchLoop() {
	defer func() {
		// Пометим, что префетчер завершился, чтобы Read мог перезапустить его при необходимости
		m.cond.L.Lock()
		m.prefetchStarted = false
		m.cond.Broadcast()
		m.cond.L.Unlock()
		close(m.prefetchDone)
	}()

	var curPos int64
	curReaderIdx := -1
	needSeek := true

	for {
		m.cond.L.Lock()
		// Остановка по Close()/флагу
		if m.closed || m.prefetchStopping {
			m.cond.L.Unlock()
			return
		}
		// Перезапуск после Seek
		if m.pfNeedSeek {
			curPos = m.pfPos
			curReaderIdx = -1
			needSeek = true
			m.pfNeedSeek = false
		}
		// Дошли до конца общего потока
		if curPos >= m.totalSize {
			if m.prefetchErr == nil {
				m.prefetchErr = io.EOF
			}
			m.cond.Broadcast()
			m.cond.L.Unlock()
			return
		}
		// Нет места в окне — ждём, пока читатель освободит слот (backpressure)
		if m.count == m.buffersNum {
			m.cond.Wait()
			m.cond.L.Unlock()
			continue
		}
		// Определяем активный источник по префиксным суммам
		if curReaderIdx < 0 || !(m.prefixSizes[curReaderIdx] <= curPos && curPos < m.prefixSizes[curReaderIdx+1]) {
			idx := sort.Search(len(m.readers), func(i int) bool { return m.prefixSizes[i+1] > curPos })
			curReaderIdx = idx
			needSeek = true
		}
		reader := m.readers[curReaderIdx]
		m.cond.L.Unlock()

		// Seek/Read выполняем без удержания лока, чтобы не блокировать читателя
		if needSeek {
			localOffset := curPos - m.prefixSizes[curReaderIdx]
			_, seekErr := reader.Seek(localOffset, io.SeekStart)
			m.cond.L.Lock()
			if m.closed || m.prefetchStopping {
				m.cond.L.Unlock()
				return
			}
			if seekErr != nil {
				m.prefetchErr = seekErr
				m.cond.Broadcast()
				m.cond.L.Unlock()
				return
			}
			needSeek = false
			m.cond.L.Unlock()
		}

		remainInReader := int(m.prefixSizes[curReaderIdx+1] - curPos)
		toRead := min(min(bufferSize, prefetchChunkLimit), remainInReader)
		buf := make([]byte, toRead)
		n, readErr := reader.Read(buf)

		m.cond.L.Lock()
		if m.closed || m.prefetchStopping {
			m.cond.L.Unlock()
			return
		}
		// Если за время I/O пришёл новый Seek — отбрасываем чанк и начинаем с новой позиции
		if m.pfNeedSeek {
			curPos = m.pfPos
			curReaderIdx = -1
			needSeek = true
			m.pfNeedSeek = false
			m.cond.L.Unlock()
			continue
		}
		if n > 0 {
			// Кладём чанк в конец окна
			m.chunks[m.tail] = chunk{buf: buf[:n], n: n}
			m.tail = (m.tail + 1) % m.buffersNum
			m.count++
			curPos += int64(n)
			m.cond.Broadcast() // разбудим читателя
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				// // Закончился текущий ридер: перейти к началу следующего, либо фиксируем общий EOF
				if curPos < m.prefixSizes[curReaderIdx+1] {
					curPos = m.prefixSizes[curReaderIdx+1]
					curReaderIdx = -1
					needSeek = true
					m.cond.L.Unlock()
					continue
				}
				// Конец общего потока
				if curPos >= m.totalSize {
					m.prefetchErr = io.EOF
					m.cond.Broadcast()
					m.cond.L.Unlock()
					return
				}
				// Иначе просто перейти к следующему ридеру
				curReaderIdx = -1
				needSeek = true
				m.cond.L.Unlock()
				continue
			}
			// Любая другая ошибка источника
			m.prefetchErr = readErr
			m.cond.Broadcast()
			m.cond.L.Unlock()
			return
		}
		m.cond.L.Unlock()
	}
}
