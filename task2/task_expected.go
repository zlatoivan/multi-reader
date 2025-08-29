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
	// bufferSize — размер одного блока префетча.
	bufferSize = 1024 * 1024
	// defaultBuffersNum — количество блоков в окне буфера.
	defaultBuffersNum = 4
)

// MultiReader объединяет несколько SizedReadSeekCloser в единый конкатенированный поток
// и поддерживает асинхронный префетч с ограниченным буфером.
type MultiReader struct {
	readers     []SizedReadSeekCloser // исходные ридеры
	totalSize   int64                 // суммарный размер
	prefixSizes []int64               // абсолютная стартовая позиция i-го ридера

	// Текущая логическая позиция чтения (для пользователя)
	absPos int64

	// Окно буфера префетча: [bufferStart, bufferStart+len(bufferData))
	bufferStart int64
	bufferData  []byte
	bufferCap   int

	// Состояние
	closed      bool
	prefetchErr error // последняя ошибка источника (включая EOF)

	// Синхронизация и управление жизненным циклом префетчера
	mu               sync.Mutex
	cond             *sync.Cond
	prefetchStarted  bool
	prefetchStopping bool
	prefetchDone     chan struct{}

	// Параметры запуска/перезапуска префетча
	pfPos      int64
	pfNeedSeek bool
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
	mr.cond = sync.NewCond(&mr.mu)

	mr.prefixSizes = make([]int64, len(readers)+1)
	var total int64
	for i, r := range mr.readers {
		mr.prefixSizes[i] = total
		total += r.Size()
	}
	mr.prefixSizes[len(readers)] = total
	mr.totalSize = total

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
func (m *MultiReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if !m.prefetchStarted {
		m.startPrefetchLocked()
	}

	n := 0
	for n < len(p) {
		if m.absPos == m.totalSize {
			if n == 0 {
				m.mu.Unlock()
				return 0, io.EOF
			}
			m.mu.Unlock()
			return n, nil
		}

		offset := m.absPos - m.bufferStart
		if offset >= 0 && offset < int64(len(m.bufferData)) {
			available := int64(len(m.bufferData)) - offset
			need := int64(len(p) - n)
			toCopy := need
			if available < toCopy {
				toCopy = available
			}
			if toCopy > 0 {
				copy(p[n:n+int(toCopy)], m.bufferData[offset:offset+toCopy])
				n += int(toCopy)
				m.absPos += toCopy

				// сдвинуть окно на уже прочитанные байты
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
				m.cond.Broadcast()
				continue
			}
		}

		if m.prefetchErr != nil {
			if n > 0 {
				m.mu.Unlock()
				return n, nil
			}
			err := m.prefetchErr
			m.mu.Unlock()
			return 0, err
		}

		if m.closed {
			m.mu.Unlock()
			return n, io.ErrClosedPipe
		}

		m.cond.Wait()
	}

	m.mu.Unlock()
	return n, nil
}

// Seek перемещает курсор. Позиции внутри текущего окна обслуживаются из буфера.
// При выходе за окно — буфер сбрасывается и префетч переинициализируется на новой позиции.
func (m *MultiReader) Seek(offset int64, whence int) (int64, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
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
		m.mu.Unlock()
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	seekPos := base + offset
	if seekPos < 0 || seekPos > m.totalSize { // позиция == totalSize допустима (EOF)
		m.mu.Unlock()
		return 0, fmt.Errorf("seek position (%d) should be >= 0 and <= totalSize (%d)", seekPos, m.totalSize)
	}

	if seekPos >= m.bufferStart && seekPos <= m.bufferStart+int64(len(m.bufferData)) {
		m.absPos = seekPos
		m.mu.Unlock()
		return seekPos, nil
	}

	// Сброс окна и переинициализация префетча
	m.absPos = seekPos
	m.bufferStart = seekPos
	m.bufferData = nil
	m.prefetchErr = nil
	m.pfPos = seekPos
	m.pfNeedSeek = true
	m.cond.Broadcast()
	m.mu.Unlock()

	return seekPos, nil
}

// Close завершает префетч и закрывает все источники, агрегируя ошибки.
func (m *MultiReader) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}

	m.closed = true
	if m.prefetchStarted && !m.prefetchStopping {
		m.prefetchStopping = true
		m.cond.Broadcast()
	}
	done := m.prefetchDone
	m.mu.Unlock()

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

// Внутреннее: запуск префетчера. Требует удержания m.mu.
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

// Внутреннее: горутина префетча.
func (m *MultiReader) prefetchLoop() {
	defer close(m.prefetchDone)

	var curPos int64
	curReaderIdx := -1
	needSeek := true

	for {
		m.mu.Lock()
		if m.closed || m.prefetchStopping {
			m.mu.Unlock()
			return
		}
		if m.pfNeedSeek {
			curPos = m.pfPos
			curReaderIdx = -1
			needSeek = true
			m.pfNeedSeek = false
		}
		if curPos >= m.totalSize {
			if m.prefetchErr == nil {
				m.prefetchErr = io.EOF
			}
			m.cond.Broadcast()
			m.mu.Unlock()
			return
		}
		if len(m.bufferData) >= m.bufferCap {
			m.cond.Wait()
			m.mu.Unlock()
			continue
		}

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

		if needSeek {
			m.mu.Unlock()
			_, seekErr := reader.Seek(localOffset, io.SeekStart)
			m.mu.Lock()
			if m.closed || m.prefetchStopping {
				m.mu.Unlock()
				return
			}
			if seekErr != nil {
				m.prefetchErr = seekErr
				m.cond.Broadcast()
				m.mu.Unlock()
				return
			}
			needSeek = false
		}

		buf := make([]byte, toRead)
		m.mu.Unlock()
		n, err := reader.Read(buf)
		if n > 0 {
			buf = buf[:n]
		} else {
			buf = buf[:0]
		}

		m.mu.Lock()
		if m.closed || m.prefetchStopping {
			m.mu.Unlock()
			return
		}
		if n > 0 {
			m.bufferData = append(m.bufferData, buf...)
			// ограничим рост, подчищая начало, но не дальше текущей позиции читателя
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
				if curPos < m.prefixSizes[curReaderIdx+1] {
					curPos = m.prefixSizes[curReaderIdx+1]
					curReaderIdx = -1
					needSeek = true
					m.mu.Unlock()
					continue
				}
				if curPos >= m.totalSize {
					m.prefetchErr = io.EOF
					m.cond.Broadcast()
					m.mu.Unlock()
					return
				}
				curReaderIdx = -1
				needSeek = true
				m.mu.Unlock()
				continue
			}
			m.prefetchErr = err
			m.cond.Broadcast()
			m.mu.Unlock()
			return
		}

		if n == 0 {
			m.cond.Broadcast()
			m.cond.Wait()
		}
		m.mu.Unlock()
	}
}
