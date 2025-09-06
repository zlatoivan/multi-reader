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
	windowBuf   []byte // текущее окно данных
	windowStart int64  // абсолютная позиция начала окна
	buffersNum  int    // ёмкость окна (в блоках)

	// Каналы и управление префетчем
	pfBufCh   chan []byte        // буферизированный канал блоков, наполняется префетчером
	pfErrCh   chan error         // канал для ошибки/EOF от префетчера (ёмкость 1)
	pfCancel  context.CancelFunc // отмена контекста префетчера
	pfDone    chan struct{}      // сигнал завершения горутины префетчера
	pfStarted bool               // флаг запуска префетчера

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
		readers:     readers,
		totalSize:   total,
		prefixSizes: prefixSizes,
		absPos:      0,
		windowBuf:   nil,
		windowStart: 0,
		buffersNum:  buffersNum,
		pfBufCh:     nil,
		pfErrCh:     nil,
		pfCancel:    nil,
		pfDone:      nil,
		pfStarted:   false,
		mu:          sync.Mutex{},
		closed:      false,
	}
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
		// Пытаемся прочитать из окна без ожидания каналов
		copied, ok := m.readFromWindow(p[n:])
		if ok {
			n += copied
			if n == len(p) {
				break
			}
			continue
		}

		// Окно пусто - ждём новый блок от префетчера
		buf, okPf := <-m.pfBufCh
		if !okPf {
			// Канал данных закрыт - считываем итоговую ошибку/EOF
			select {
			case err = <-m.pfErrCh:
				if err == nil { // канал ошибок закрыт без значения
					err = io.EOF
				}
			default:
				err = io.EOF
			}
			return n, err
		}
		m.mu.Lock()
		m.windowBuf = append(m.windowBuf, buf...)
		m.mu.Unlock()
	}

	return n, nil
}

// Seek перемещает курсор. Если позиция внутри текущего окна - переиндексируем окно.
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

	delta := seekPos - m.windowStart
	switch {
	// Быстрый путь: позиция внутри текущего окна - только сдвигаем смещение
	case 0 <= delta && delta < int64(len(m.windowBuf)):
		m.windowBuf = m.windowBuf[delta:]
	// Вне окна: сбрасываем окно и перезапускаем префетч при следующем чтении
	default:
		m.windowBuf = nil
		if m.pfStarted {
			m.resetPrefetchLocked()
		}
	}

	m.windowStart = seekPos
	m.absPos = seekPos

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
			multiErr = errors.Join(multiErr, err)
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

// prefetchLoop - горутина префетча. Наполняет pfBufCh блоками, по завершении шлёт ошибку в pfErrCh.
func (m *MultiReader) prefetchLoop(ctx context.Context, startPos int64) {
	pfBufCh := m.pfBufCh // Локальные копии каналов для безопасного закрытия без гонок
	pfDone := m.pfDone
	pfErrCh := m.pfErrCh
	defer func() {
		close(pfDone)
		close(pfBufCh)
		close(pfErrCh)
	}()

	curPos := startPos
	curReaderIdx := -1
	needSeek := true

	for {
		// Общий EOF: больше данных не будет, уведомляем и завершаемся
		if curPos >= m.totalSize {
			sendErr(pfErrCh, io.EOF)
			return
		}

		// Выбор активного ридера и установка needSeek
		if curReaderIdx < 0 || !(m.prefixSizes[curReaderIdx] <= curPos && curPos < m.prefixSizes[curReaderIdx+1]) {
			curReaderIdx = sort.Search(len(m.readers), func(i int) bool { return m.prefixSizes[i+1] > curPos })
			needSeek = true
		}
		reader := m.readers[curReaderIdx]

		// Выполнение Seek и сброс needSeek
		if needSeek {
			localOffset := curPos - m.prefixSizes[curReaderIdx]
			_, err := reader.Seek(localOffset, io.SeekStart)
			if err != nil {
				sendErr(pfErrCh, err)
				return
			}
			needSeek = false
		}

		// Выполнение Read
		nextReader := func() {
			curPos = m.prefixSizes[curReaderIdx+1]
			curReaderIdx = -1
			needSeek = true
		}
		remainInReader := int(m.prefixSizes[curReaderIdx+1] - curPos)
		if remainInReader == 0 { // Достигли границы ридеров
			nextReader()
			continue
		}
		toRead := min(remainInReader, bufferSize)
		buf := make([]byte, toRead)
		n, err := reader.Read(buf)
		if n > 0 {
			select {
			case <-ctx.Done():
				return
			case pfBufCh <- buf[:n]: // Ждем, пока окно освободиться, чтобы записать следующий блок
				curPos += int64(n) // Обновляем глобальную позицию на фактически прочитанные байты
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) { // Достигли конца этого ридера
				nextReader()
				continue
			}
			sendErr(pfErrCh, err)
			return
		}
	}
}

// readFromWindow копирует данные из окна в dst под локом. Возвращает (copied, true), если данные были.
func (m *MultiReader) readFromWindow(dst []byte) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Окно пусто - данных нет
	if len(m.windowBuf) == 0 {
		return 0, false
	}

	// Копируем и продвигаем курсоры, используя «срез с головы»
	toCopy := min(len(dst), len(m.windowBuf))
	copy(dst[:toCopy], m.windowBuf[:toCopy])
	m.windowBuf = m.windowBuf[toCopy:]
	m.windowStart += int64(toCopy)
	m.absPos += int64(toCopy)

	return toCopy, true
}

// sendErr отправляет ошибку в канал, если есть место
func sendErr(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

// resetPrefetchLocked останавливает текущий префетч и сбрасывает его поля. Требует удержания m.mu
func (m *MultiReader) resetPrefetchLocked() {
	if m.pfCancel != nil {
		m.pfCancel()
	}
	oldDone := m.pfDone
	if oldDone != nil { // Дождаться завершения старого префетчера, чтобы исключить параллельный доступ
		<-oldDone
	}
	m.pfStarted = false
	m.pfBufCh = nil
	m.pfErrCh = nil
	m.pfDone = nil
	m.pfCancel = nil
}
