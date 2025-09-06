package main

import (
	"context"
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
	// bufferSize - размер одного блока префетча.
	bufferSize = 1024 * 1024
	// defaultBuffersNum - количество блоков в окне буфера.
	defaultBuffersNum = 4
)

// MultiReader объединяет несколько SizedReadSeekCloser в единый конкатенированный поток
// и поддерживает асинхронный префетч с ограниченным буфером через каналы.
// Внешний Read отдаёт байты из внутреннего окна, которое наполняется фоном.
type MultiReader struct {
	readers     []SizedReadSeekCloser // исходные ридеры
	totalSize   int64                 // суммарный размер всех источников
	prefixSizes []int64               // абсолютные стартовые позиции ридеров (префиксные суммы)
	absPos      int64                 // абсолютная позиция курсора чтения (пользователя)

	// Окно префетча
	window         [][]byte // голова = window[0]
	headStart      int64    // абсолютная позиция начала window[0]
	consumedInHead int      // сколько байт уже отдано из window[0]
	buffersNum     int      // ёмкость окна (в блоках)

	// Каналы и управление префетчем
	pfBufCh   chan []byte
	pfErrCh   chan error
	pfCancel  context.CancelFunc
	pfDone    chan struct{}
	pfStarted bool

	// Состояние/синхронизация
	mu     sync.Mutex
	closed bool
}

// Проверка, что MultiReader удовлетворяет интерфейсу SizedReadSeekCloser
var _ SizedReadSeekCloser = (*MultiReader)(nil)

// NewMultiReader создаёт конкатенированный ридер поверх набора SizedReadSeekCloser
// с поддержкой асинхронного префетча по каналам.
func NewMultiReader(buffersNum int, readers ...SizedReadSeekCloser) *MultiReader {
	if buffersNum <= 0 {
		buffersNum = defaultBuffersNum
	}

	prefixSizes := make([]int64, len(readers)+1)
	var total int64
	for i, r := range readers {
		prefixSizes[i] = total
		total += r.Size()
	}
	prefixSizes[len(readers)] = total

	return &MultiReader{
		readers:        readers,
		totalSize:      total,
		prefixSizes:    prefixSizes,
		absPos:         0,
		window:         nil,
		headStart:      0,
		consumedInHead: 0,
		buffersNum:     buffersNum,
		pfBufCh:        nil,
		pfErrCh:        nil,
		pfCancel:       nil,
		pfDone:         nil,
		pfStarted:      false,
		mu:             sync.Mutex{},
		closed:         false,
	}
}

// Size возвращает суммарный размер всех ридеров.
func (m *MultiReader) Size() int64 {
	return m.totalSize
}

// Read читает данные из внутреннего окна, пополняемого префетчером.
func (m *MultiReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if m.absPos == m.totalSize {
		m.mu.Unlock()
		return 0, io.EOF
	}
	if !m.pfStarted {
		m.startPrefetchLocked(m.absPos)
	}
	m.mu.Unlock()

	for n < len(p) {
		// Быстрое копирование из головы окна, атомарно под локом
		if copied, ok := m.readFromWindow(p[n:]); ok {
			n += copied
			if n == len(p) {
				break
			}
			continue
		}

		// Окно пусто — ждём новые блоки или ошибку/закрытие
		select {
		case buf, ok := <-m.pfBufCh:
			if !ok {
				// Канал буферов закрыт — читаем ошибку/EOF
				select {
				case err = <-m.pfErrCh:
				default:
					err = io.EOF
				}
				if n == 0 {
					return 0, err
				}
				return n, err
			}
			m.appendTail(buf)
			continue
		}
	}

	return n, nil
}

// Seek перемещает курсор. Если позиция внутри текущего окна — переиндексируем окно.
// Иначе сбрасываем окно и перезапускаем префетч с новой позиции лениво.
func (m *MultiReader) Seek(offset int64, whence int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

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

	// Попытка выполнить быстрый seek внутри окна
	if len(m.window) > 0 {
		// 1) Внутри текущего head-буфера — просто двигаем consumedInHead
		headLen := len(m.window[0])
		headEnd := m.headStart + int64(headLen)
		if seekPos >= m.headStart && seekPos < headEnd {
			m.consumedInHead = int(seekPos - m.headStart)
			m.absPos = seekPos
			return seekPos, nil
		}

		effectiveStart := m.headStart + int64(m.consumedInHead)
		var windowEnd int64 = m.headStart
		for _, b := range m.window {
			windowEnd += int64(len(b))
		}
		if seekPos >= effectiveStart && seekPos < windowEnd {
			cur := m.headStart
			idx := 0
			consumed := 0
			for idx < len(m.window) {
				next := cur + int64(len(m.window[idx]))
				if seekPos < next {
					consumed = int(seekPos - cur)
					break
				}
				cur = next
				idx++
			}
			if idx < len(m.window) {
				m.window = m.window[idx:]
				m.headStart = cur
				m.consumedInHead = consumed
				m.absPos = seekPos
				return seekPos, nil
			}
		}
	}

	// Вне окна — сбрасываем и перезапустим префетч при следующем Read
	m.window = nil
	m.headStart = seekPos
	m.consumedInHead = 0
	m.absPos = seekPos
	if m.pfStarted {
		if m.pfCancel != nil {
			m.pfCancel()
		}
		m.pfStarted = false
		m.pfBufCh = nil
		m.pfErrCh = nil
		m.pfDone = nil
		m.pfCancel = nil
	}

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
	if m.pfCancel != nil {
		m.pfCancel()
	}
	pfDone := m.pfDone
	m.mu.Unlock()

	if pfDone != nil {
		<-pfDone
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

// startPrefetchLocked запускает горутину префетчера, читающую блоки в каналы.
func (m *MultiReader) startPrefetchLocked(startPos int64) {
	if m.pfStarted {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.pfBufCh = make(chan []byte, m.buffersNum)
	m.pfErrCh = make(chan error, 1)
	m.pfCancel = cancel
	m.pfDone = make(chan struct{})
	m.pfStarted = true
	go m.prefetchLoop(ctx, startPos)
}

// prefetchLoop — горутина префетча. Наполняет pfBufCh блоками, по завершении шлёт ошибку в pfErrCh.
func (m *MultiReader) prefetchLoop(ctx context.Context, startPos int64) {
	defer func() {
		close(m.pfDone)
		close(m.pfBufCh)
	}()

	curPos := startPos
	curReaderIdx := -1
	needSeek := true

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if curPos >= m.totalSize {
			select {
			case m.pfErrCh <- io.EOF:
			default:
			}
			return
		}

		if curReaderIdx < 0 || !(m.prefixSizes[curReaderIdx] <= curPos && curPos < m.prefixSizes[curReaderIdx+1]) {
			curReaderIdx = sort.Search(len(m.readers), func(i int) bool { return m.prefixSizes[i+1] > curPos })
			needSeek = true
		}
		reader := m.readers[curReaderIdx]

		if needSeek {
			localOffset := curPos - m.prefixSizes[curReaderIdx]
			_, err := reader.Seek(localOffset, io.SeekStart)
			if err != nil {
				select {
				case m.pfErrCh <- err:
				default:
				}
				return
			}
			needSeek = false
		}

		remainInReader := int(m.prefixSizes[curReaderIdx+1] - curPos)
		if remainInReader == 0 {
			curPos = m.prefixSizes[curReaderIdx+1]
			curReaderIdx = -1
			needSeek = true
			continue
		}
		toRead := min(bufferSize, remainInReader)
		buf := make([]byte, toRead)
		n, readErr := reader.Read(buf)
		if n > 0 {
			select {
			case <-ctx.Done():
				return
			case m.pfBufCh <- buf[:n]:
				curPos += int64(n)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				if curPos >= m.totalSize {
					select {
					case m.pfErrCh <- io.EOF:
					default:
					}
					return
				}
				if curPos < m.prefixSizes[curReaderIdx+1] {
					curPos = m.prefixSizes[curReaderIdx+1]
				}
				curReaderIdx = -1
				needSeek = true
				continue
			}
			select {
			case m.pfErrCh <- readErr:
			default:
			}
			return
		}
	}
}

// appendTail добавляет буфер в окно, обновляя начало окна при необходимости.
func (m *MultiReader) appendTail(buf []byte) {
	m.mu.Lock()
	if len(m.window) == 0 {
		m.headStart = m.absPos - int64(m.consumedInHead)
		if m.headStart < 0 {
			m.headStart = 0
		}
	}
	m.window = append(m.window, buf)
	m.mu.Unlock()
}

// readFromWindow копирует данные из окна в dst под локом. Возвращает (copied, true), если данные были.
func (m *MultiReader) readFromWindow(dst []byte) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.window) == 0 {
		return 0, false
	}
	head := m.window[0]
	available := len(head) - m.consumedInHead
	if available <= 0 {
		m.headStart += int64(len(head))
		m.window = m.window[1:]
		m.consumedInHead = 0
		return 0, false
	}
	toCopy := len(dst)
	if toCopy > available {
		toCopy = available
	}
	copy(dst[:toCopy], head[m.consumedInHead:m.consumedInHead+toCopy])
	m.consumedInHead += toCopy
	m.absPos += int64(toCopy)
	if m.consumedInHead == len(head) {
		m.headStart += int64(len(head))
		m.window = m.window[1:]
		m.consumedInHead = 0
	}
	return toCopy, true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
