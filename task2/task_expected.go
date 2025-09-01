package main

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
)

// SizedReadSeekCloser - интерфейс ридера с возможностью seek и знанием своего размера.
type SizedReadSeekCloser interface {
	io.ReadSeekCloser
	Size() int64
}

const (
	// bufferSize - размер одного блока префетча. Это «порция» чтения префетчером; влияет на частоту I/O.
	bufferSize = 1024 * 1024
	// defaultBuffersNum - количество блоков в окне буфера. Итоговая ёмкость буфера: defaultBuffersNum * bufferSize.
	defaultBuffersNum = 4
)

// MultiReader объединяет несколько SizedReadSeekCloser в единый конкатенированный поток
// и поддерживает асинхронный префетч с ограниченным буфером.
// Внешний Read отдаёт байты только из внутреннего буфера, который наполняется фоном.
type MultiReader struct {
	readers     []SizedReadSeekCloser // исходные ридеры
	totalSize   int64                 // суммарный размер всех источников
	prefixSizes []int64               // абсолютные стартовые позиции ридеров (префиксные суммы)
	absPos      int64                 // абсолютная позиция курсора чтения (пользователя). Меняется при Read/Seek.

	// Кольцевое окно из буферов фиксированного размера
	buffers        [][]byte // слоты кольца; каждый слот - независимый буфер данных
	headStart      int64    // абсолютная позиция начала буферного окна. Меняется при полном «съедании» головы или при сбросе окна из‑за Seek вне окна
	headIdx        int      // индекс головы (откуда читает потребитель)
	tailIdx        int      // индекс хвоста (куда пишет префетчер)
	occupiedCnt    int      // сколько слотов сейчас занято данными (0..buffersNum)
	consumedInHead int      // сколько байт уже отдано из головного буфера

	// Состояние
	closed      bool  // MultiReader закрыт пользователем
	prefetchErr error // последняя ошибка источника (включая EOF)

	// Синхронизация и управление жизненным циклом префетчера
	cond            *sync.Cond    // условная переменная; используем cond.L как единственный локер
	prefetchStarted bool          // запущена ли горутина префетчера
	prefetchDone    chan struct{} // канал-сигнал завершения горутины префетчера

	// Параметры запуска/перезапуска префетча
	pfPos      int64 // позиция, с которой префетчер должен начать читать
	pfNeedSeek bool  // префетчер должен сделать Seek на pfPos перед чтением
}

// ringOps - небольшой помощник для операций над кольцевым буфером.
// Все методы должны вызываться под локом m.cond.L.
type ringOps struct {
	m *MultiReader
}

// inHead проверяет, попадает ли позиция внутрь текущего головного буфера.
func (r ringOps) inHead(pos int64) bool {
	m := r.m
	if m.occupiedCnt == 0 {
		return false
	}

	headEnd := m.headStart + int64(len(m.buffers[m.headIdx]))
	if m.headStart <= pos && pos < headEnd {
		return true
	}

	return false
}

// seekWithinWindow пытается найти позицию внутри эффективного окна (остаток головы + хвостовые буферы),
// и если находит - переиндексирует head/consumedInHead так, чтобы следующая выдача началась от pos.
func (r ringOps) seekWithinWindow(pos int64) bool {
	m := r.m
	if m.occupiedCnt == 0 {
		return false
	}

	effectiveStart := m.headStart + int64(m.consumedInHead)
	if pos < effectiveStart {
		return false
	}

	seekDelta := int(pos - effectiveStart)
	headIdx := m.headIdx
	consumed := m.consumedInHead
	for range m.occupiedCnt {
		remain := len(m.buffers[headIdx]) - consumed
		if seekDelta < remain {
			m.headIdx = headIdx
			m.consumedInHead = consumed + seekDelta
			return true
		}
		seekDelta -= remain
		headIdx = (headIdx + 1) % len(m.buffers)
		consumed = 0
	}

	return false
}

// resetTo полностью очищает окно и подготавливает префетч к чтению с новой позиции pos.
func (r ringOps) resetTo(pos int64) {
	m := r.m
	for i := 0; i < m.occupiedCnt; i++ {
		m.buffers[(m.headIdx+i)%len(m.buffers)] = nil
	}

	m.tailIdx = m.headIdx
	m.occupiedCnt = 0
	m.consumedInHead = 0
	m.headStart = pos
	m.prefetchErr = nil
	m.pfPos = pos
	m.pfNeedSeek = true
	m.cond.Broadcast()
}

// Проверка, что MultiReader удовлетворяет интерфейсу SizedReadSeekCloser
var _ SizedReadSeekCloser = (*MultiReader)(nil)

// NewMultiReader создаёт конкатенированный ридер поверх набора SizedReadSeekCloser
// с поддержкой асинхронного префетча. Префетчер запускается лениво при первом чтении.
func NewMultiReader(buffersNum int, readers ...SizedReadSeekCloser) *MultiReader {
	if buffersNum <= 0 {
		buffersNum = defaultBuffersNum
	}

	// Считаем префиксные суммы и общий размер заранее - понадобится для быстрого поиска активного ридера
	prefixSizes := make([]int64, len(readers)+1)
	var total int64
	for i, r := range readers {
		prefixSizes[i] = total
		total += r.Size()
	}
	prefixSizes[len(readers)] = total

	return &MultiReader{
		readers:         readers,
		totalSize:       total,
		prefixSizes:     prefixSizes,
		absPos:          0,
		buffers:         make([][]byte, buffersNum), // кольцевой буфер буферов фиксированной длины
		headStart:       0,
		headIdx:         0,
		tailIdx:         0,
		occupiedCnt:     0,
		consumedInHead:  0,
		closed:          false,
		prefetchErr:     nil,
		cond:            sync.NewCond(&sync.Mutex{}),
		prefetchStarted: false,
		prefetchDone:    nil,
		pfPos:           0,
		pfNeedSeek:      true,
	}
}

// Read читает данные из внутреннего буфера (очереди буферов).
func (m *MultiReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	switch {
	case m.closed:
		return 0, io.ErrClosedPipe
	case m.absPos == m.totalSize: // Если уже на EOF, не запускаем префетчер зря
		return 0, io.EOF
	case !m.prefetchStarted: // Ленивая инициализация префетчера - до первого реального чтения
		m.startPrefetchLocked()
	}

	for n < len(p) {
		switch {
		case m.absPos == m.totalSize: // Достигнут общий EOF - больше данных не будет
			return n, io.EOF
		case m.occupiedCnt == 0 || m.consumedInHead >= len(m.buffers[m.headIdx]): // Данных нет либо голова исчерпана - ждём пополнения/ошибки
			if m.prefetchErr != nil {
				return n, m.prefetchErr
			}
			m.cond.Wait()
			continue
		}

		// Копируем из головного буфера и сдвигаем окно
		headBuf := m.buffers[m.headIdx]
		toCopy := len(headBuf) - m.consumedInHead
		need := len(p) - n
		if toCopy > need {
			toCopy = need
		}
		copy(p[n:n+toCopy], headBuf[m.consumedInHead:m.consumedInHead+toCopy])
		n += toCopy
		m.absPos += int64(toCopy)
		m.consumedInHead += toCopy

		// Если головной буфер закончился - освобождаем слот и сдвигаем head
		if m.consumedInHead == len(headBuf) {
			m.headStart += int64(len(headBuf))
			m.headIdx = (m.headIdx + 1) % len(m.buffers)
			m.occupiedCnt--
			m.consumedInHead = 0
			m.buffers[(m.headIdx+len(m.buffers)-1)%len(m.buffers)] = nil // Очищаем слот, который был головой
			m.cond.Broadcast()                                           // будим префетчер, если он ждал свободного слота
		}
	}

	return n, nil
}

// Seek перемещает курсор. Если позиция внутри текущего окна - переиндексируем буфер/смещение.
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

	ro := ringOps{m: m}
	switch {
	// 1) Внутри head-буфера - просто двигаем consumedInHead
	case ro.inHead(seekPos):
		m.consumedInHead = int(seekPos - m.headStart)
	// 2) Внутри эффективного окна - переиндексируем head/consumedInHead
	case ro.seekWithinWindow(seekPos):
	// 3) Вне окна - сбрасываем очередь и перезапускаем префетч с новой позиции
	default:
		ro.resetTo(seekPos)
	}

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
	m.cond.Broadcast()
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

// Size возвращает суммарный размер всех ридеров.
func (m *MultiReader) Size() int64 {
	return m.totalSize
}

// startPrefetchLocked - запуск префетчера. Требует удержания блокировки cond.L.
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

// consumePendingSeek применяет отложенный запрос Seek для префетчера. Требует удержания m.cond.L.
func (m *MultiReader) consumePendingSeek(curPos *int64, curReaderIdx *int, needSeek *bool) {
	*curPos = m.pfPos
	*curReaderIdx = -1
	*needSeek = true
	m.pfNeedSeek = false
}

// prefetchLoop - горутина префетча. Наполняет очередь буферов.
func (m *MultiReader) prefetchLoop() {
	defer func() {
		// Пометим, что префетчер завершился, чтобы Read мог перезапустить его при необходимости; и чтобы Close увидел
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
		if m.closed {
			m.cond.L.Unlock()
			return
		}

		// Перезапуск после Seek
		if m.pfNeedSeek {
			m.consumePendingSeek(&curPos, &curReaderIdx, &needSeek)
		}

		// Дошли до конца общего потока - будим читателей и выходим
		if curPos >= m.totalSize {
			if m.prefetchErr == nil {
				m.prefetchErr = io.EOF
			}
			m.cond.Broadcast()
			m.cond.L.Unlock()
			return
		}

		// Нет места в окне - ждём, пока читатель освободит слот (backpressure)
		if m.occupiedCnt == len(m.buffers) {
			m.cond.Wait()
			m.cond.L.Unlock()
			continue
		}

		// Определяем индекс активного ридера по префиксным суммам
		if curReaderIdx < 0 || !(m.prefixSizes[curReaderIdx] <= curPos && curPos < m.prefixSizes[curReaderIdx+1]) {
			curReaderIdx = sort.Search(len(m.readers), func(i int) bool {
				return m.prefixSizes[i+1] > curPos
			})
			needSeek = true
		}
		reader := m.readers[curReaderIdx]
		m.cond.L.Unlock()

		// Seek/Read выполняем без удержания лока, чтобы не блокировать читателя
		if needSeek {
			localOffset := curPos - m.prefixSizes[curReaderIdx]
			_, seekErr := reader.Seek(localOffset, io.SeekStart)
			m.cond.L.Lock()
			if m.closed {
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
		// Блок пустой - переходим к следующему ридеру, не делая чтения нулевого блока (избегаем бесконечного цикла)
		if remainInReader == 0 {
			curPos = m.prefixSizes[curReaderIdx+1]
			curReaderIdx = -1
			needSeek = true
			continue
		}
		toRead := min(bufferSize, remainInReader)
		buf := make([]byte, toRead)
		n, readErr := reader.Read(buf)

		m.cond.L.Lock()
		if m.closed {
			m.cond.L.Unlock()
			return
		}

		// Если за время I/O пришёл новый Seek - отбрасываем буфер и начинаем с новой позиции
		if m.pfNeedSeek {
			m.consumePendingSeek(&curPos, &curReaderIdx, &needSeek)
			m.cond.L.Unlock()
			continue
		}
		if n > 0 {
			// Кладём буфер в конец окна
			m.buffers[m.tailIdx] = buf[:n]
			m.tailIdx = (m.tailIdx + 1) % len(m.buffers)
			m.occupiedCnt++
			curPos += int64(n)
			m.cond.Broadcast() // разбудим читателя
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				// Общий EOF, если дошли до конца всего потока
				if curPos >= m.totalSize {
					if m.prefetchErr == nil {
						m.prefetchErr = io.EOF
					}
					m.cond.Broadcast()
					m.cond.L.Unlock()
					return
				}

				// Иначе - перейти к следующему ридеру (при необходимости подровняв curPos к границе текущего)
				if curPos < m.prefixSizes[curReaderIdx+1] {
					curPos = m.prefixSizes[curReaderIdx+1]
				}
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
