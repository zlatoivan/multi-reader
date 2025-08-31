// Пакет main содержит реализацию MultiReader с асинхронным префетчем (фоновой подкачкой)
// для объединённого чтения из нескольких источников данных как из одного потока.
package main

import (
	"errors" // объединение ошибок и проверка io.EOF
	"fmt"    // форматирование сообщений об ошибках
	"io"     // стандартные интерфейсы ввода/вывода
	"sort"   // бинарный поиск по префиксным суммам
	"sync"   // мьютексы и условные переменные
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
	// Итоговая ёмкость буфера: bufferSize * defaultBuffersNum.
	defaultBuffersNum = 4
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

	// Окно буфера префетча: [bufferStart, bufferStart+len(bufferData))
	// Все чтения идут только из этого окна.
	bufferStart int64
	bufferData  []byte
	bufferCap   int

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

// Проверка, что MultiReader удовлетворяет интерфейсу SizedReadSeekCloser
var _ SizedReadSeekCloser = (*MultiReader)(nil)

// NewMultiReader создаёт конкатенированный ридер поверх набора SizedReadSeekCloser
// с поддержкой асинхронного префетча. Префетчер запускается лениво при первом чтении.
func NewMultiReader(readers ...SizedReadSeekCloser) *MultiReader {
	mr := &MultiReader{
		readers:    readers,
		bufferCap:  bufferSize * defaultBuffersNum,
		pfNeedSeek: true,
	}
	// Привязываем cond к мьютексу — это механизм ожидания/пробуждения
	mr.cond = sync.NewCond(&sync.Mutex{})

	// Подготовим префиксные суммы и totalSize
	mr.prefixSizes = make([]int64, len(readers)+1)
	var total int64
	for i, r := range mr.readers {
		mr.prefixSizes[i] = total
		total += r.Size()
	}
	mr.prefixSizes[len(readers)] = total
	mr.totalSize = total

	// Начальные значения для курсора и буфера
	mr.absPos = 0
	mr.bufferStart = 0
	mr.bufferData = nil
	mr.prefetchErr = nil
	mr.pfPos = 0

	return mr
}

// Size возвращает суммарный размер всех ридеров.
func (m *MultiReader) Size() int64 {
	return m.totalSize
}

// Read читает данные из внутреннего буфера. Префетч запускается при первом чтении.
// Алгоритм:
// 1) Если буфер содержит нужные байты — копируем их и сдвигаем окно.
// 2) Если буфер пуст и нет ошибки — ждём пополнения от префетчера.
// 3) Если префетчер установил ошибку/EOF — возвращаем частично прочитанное или ошибку.
func (m *MultiReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	if m.closed {
		return 0, io.ErrClosedPipe
	}
	// Ленивая инициализация префетчера
	if !m.prefetchStarted {
		m.startPrefetchLocked()
	}

	n := 0
	for n < len(p) {
		// Достигнут общий EOF
		if m.absPos == m.totalSize {
			if n == 0 {
				return 0, io.EOF
			}
			return n, nil
		}

		// Пытаемся прочитать из окна
		offset := m.absPos - m.bufferStart
		if offset >= 0 && offset < int64(len(m.bufferData)) {
			available := int64(len(m.bufferData)) - offset
			need := int64(len(p) - n)
			toCopy := need
			if available < toCopy {
				toCopy = available
			}
			if toCopy > 0 {
				// Копируем из буфера наружу
				copy(p[n:n+int(toCopy)], m.bufferData[offset:offset+toCopy])
				n += int(toCopy)
				m.absPos += toCopy

				// Сдвигаем окно на уже прочитанные байты
				consumed := m.absPos - m.bufferStart
				if consumed > 0 {
					if consumed >= int64(len(m.bufferData)) {
						m.bufferData = nil
						m.bufferStart = m.absPos
					} else {
						m.bufferData = m.bufferData[consumed:]
						m.bufferStart += consumed
					}
				}
				// Освободили место — будим префетч
				m.cond.Broadcast()
				continue
			}
		}

		// Переход к ошибке/EOF от префетчера
		if m.prefetchErr != nil {
			if n > 0 {
				return n, nil
			}
			err := m.prefetchErr
			return 0, err
		}

		// Пока данных нет — ждём пополнение
		if m.closed {
			return n, io.ErrClosedPipe
		}

		m.cond.Wait()
	}

	return n, nil
}

// Seek перемещает курсор. Позиции внутри текущего окна обслуживаются из буфера.
// При выходе за окно — сброс буфера и переинициализация префетча с новой позиции.
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
	if seekPos < 0 || seekPos > m.totalSize { // позиция == totalSize допустима (EOF)
		return 0, fmt.Errorf("seek position (%d) should be >= 0 and <= totalSize (%d)", seekPos, m.totalSize)
	}

	// Внутри окна — просто двигаем абс. позицию
	if seekPos >= m.bufferStart && seekPos <= m.bufferStart+int64(len(m.bufferData)) {
		m.absPos = seekPos
		return seekPos, nil
	}

	// Вне окна — сброс и команда префетчеру начать с новой позиции
	m.absPos = seekPos
	m.bufferStart = seekPos
	m.bufferData = nil
	m.prefetchErr = nil
	m.pfPos = seekPos
	m.pfNeedSeek = true
	m.cond.Broadcast()

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

// Внутреннее: горутина префетча. Читает порциями вперёд и добавляет данные в буфер.
// Соблюдает backpressure: если буфер полон — ждёт; если данных нет — добавляет и будит читателя.
func (m *MultiReader) prefetchLoop() {
	defer close(m.prefetchDone)

	var curPos int64
	curReaderIdx := -1
	needSeek := true

	for {
		m.cond.L.Lock()
		// Остановка по Close() или явному флагу
		if m.closed || m.prefetchStopping {
			m.cond.L.Unlock()
			return
		}
		// Перезапуск (после Seek)
		if m.pfNeedSeek {
			curPos = m.pfPos
			curReaderIdx = -1
			needSeek = true
			m.pfNeedSeek = false
		}
		// Конец общего потока
		if curPos >= m.totalSize {
			if m.prefetchErr == nil {
				m.prefetchErr = io.EOF
			}
			m.cond.Broadcast()
			m.cond.L.Unlock()
			return
		}
		// Буфер заполнен — ждём, когда читатель освободит место
		if len(m.bufferData) >= m.bufferCap {
			m.cond.Wait()
			m.cond.L.Unlock()
			continue
		}

		// Рассчитаем размер порции и активный ридер по префиксным суммам
		spaceLeft := m.bufferCap - len(m.bufferData)
		toRead := bufferSize
		if toRead > spaceLeft {
			toRead = spaceLeft
		}
		if curReaderIdx < 0 || !(m.prefixSizes[curReaderIdx] <= curPos && curPos < m.prefixSizes[curReaderIdx+1]) {
			idx := sort.Search(len(m.readers), func(i int) bool { return m.prefixSizes[i+1] > curPos })
			curReaderIdx = idx
			needSeek = true
		}
		localOffset := curPos - m.prefixSizes[curReaderIdx]
		remainInReader := int(m.prefixSizes[curReaderIdx+1] - curPos)
		if toRead > remainInReader {
			toRead = remainInReader
		}
		reader := m.readers[curReaderIdx]

		// Seek/Read выполняем вне критической секции, чтобы не блокировать читателя
		if needSeek {
			m.cond.L.Unlock()
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
		}

		buf := make([]byte, toRead)
		m.cond.L.Unlock()
		n, err := reader.Read(buf)
		if n > 0 {
			buf = buf[:n]
		} else {
			buf = buf[:0]
		}

		m.cond.L.Lock()
		if m.closed || m.prefetchStopping {
			m.cond.L.Unlock()
			return
		}
		if n > 0 {
			// Добавляем данные в конец окна и, если нужно, подчищаем голову
			m.bufferData = append(m.bufferData, buf...)
			overflow := len(m.bufferData) - m.bufferCap
			if overflow > 0 {
				maxTrim := int(m.absPos - m.bufferStart)
				if maxTrim < 0 {
					maxTrim = 0
				}
				if overflow > maxTrim {
					overflow = maxTrim
				}
				if overflow > 0 {
					m.bufferData = m.bufferData[overflow:]
					m.bufferStart += int64(overflow)
				}
			}
			curPos += int64(n)
			m.cond.Broadcast()
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				// Закончился текущий ридер: перейти к началу следующего
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
			// Любая другая ошибка
			m.prefetchErr = err
			m.cond.Broadcast()
			m.cond.L.Unlock()
			return
		}

		if n == 0 {
			// Избежим холостого цикла: кратко подождём сигналов
			m.cond.Broadcast()
			m.cond.Wait()
		}
		m.cond.L.Unlock()
	}
}
