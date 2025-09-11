package main

import (
	"errors"
	"fmt"
	"io"
	"sort"
)

// SizedReadSeekCloser - интерфейс ридера с возможностью seek и знанием своего размера.
type SizedReadSeekCloser interface {
	io.ReadSeekCloser
	Size() int64
}

// MultiReader объединяет несколько SizedReadSeekCloser в единый конкатенированный поток.
type MultiReader struct {
	readers     []SizedReadSeekCloser // Содержит исходные ридеры в порядке конкатенации
	totalSize   int64                 // Суммарный размер всех ридеров, вычисляется один раз в NewMultiReader
	prefixSizes []int64               // prefixSizes[i] - абсолютная стартовая позиция i-го ридера (длина = len(readers)+1)
	absPos      int64                 // Абсолютная позиция в объединённом потоке
	needSeek    bool                  // Флаг - нужно ли выставить позицию перед следующим чтением
	closed      bool                  // Флаг - MultiReader закрыт и дальнейшие операции недоступны
}

// NewMultiReader создаёт конкатенированный ридер поверх набора SizedReadSeekCloser.
func NewMultiReader(readers ...SizedReadSeekCloser) *MultiReader {
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
		needSeek:    true,
		closed:      false,
	}
}

// Проверка, что MultiReader удовлетворяет интерфейсу SizedReadSeekCloser
var _ SizedReadSeekCloser = (*MultiReader)(nil)

// Read читает данные последовательно из всех ридеров в порядке передачи в NewMultiReader.
func (m *MultiReader) Read(p []byte) (n int, err error) {
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if m.absPos == m.totalSize {
		return 0, io.EOF
	}

	for n < len(p) {
		if m.absPos == m.totalSize { // Уже что-то прочитали в этом вызове (n > 0), а затем дошли до конца
			return n, nil
		}

		// Определить текущий ридер по абсолютной позиции
		i := sort.Search(len(m.readers), func(i int) bool {
			return m.prefixSizes[i+1] > m.absPos
		})
		reader := m.readers[i]

		if m.needSeek {
			localOffset := m.absPos - m.prefixSizes[i]
			_, seekErr := reader.Seek(localOffset, io.SeekStart)
			switch {
			case seekErr != nil && n > 0: // Уже успели что-то прочитать - вернуть n и ошибку
				return n, seekErr
			case seekErr != nil: // Еще не успели ничего прочитать - вернуть 0 и ошибку
				return 0, seekErr
			}
			m.needSeek = false
		}

		k, readErr := reader.Read(p[n:])
		if k > 0 {
			n += k
			m.absPos += int64(k)
		}

		switch {
		case readErr == nil && k == 0: // Текущий ридер не продвинулся и не вернул ошибку. Выходим, чтобы не зациклиться
			return n, nil
		case readErr == nil: // Прочитали k > 0 байт без ошибки. Пытаемся дочитать дальше
			continue
		case errors.Is(readErr, io.EOF): // Текущий ридер закончился. Не возвращаем EOF сразу, а переходим к след. ридеру.
			m.absPos = m.prefixSizes[i+1] // Перейти к началу следующего ридера
			m.needSeek = true
			continue
		default: // Любая другая ошибка
			return n, readErr
		}
	}

	return n, nil
}

// Seek перемещает курсор в объединённой последовательности ридеров.
func (m *MultiReader) Seek(offset int64, whence int) (int64, error) {
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
	if seekPos < 0 || seekPos > m.totalSize { // Позиция, равная totalSize, допустима (это валидный EOF).
		return 0, fmt.Errorf("seek position (%d) should be >= 0 and <= totalSize (%d)", seekPos, m.totalSize)
	}

	m.absPos = seekPos
	m.needSeek = seekPos != m.totalSize // Ленивый seek: фактический Seek в нижнем ридере произойдёт при первом Read

	return seekPos, nil
}

// Close закрывает все ридеры, объединяя все ошибки.
func (m *MultiReader) Close() error {
	if m.closed {
		return nil
	}

	var multiErr error
	for _, r := range m.readers {
		err := r.Close()
		if err != nil {
			multiErr = errors.Join(err, multiErr)
		}
	}

	m.closed = true

	if multiErr != nil {
		return fmt.Errorf("error when closing: %w", multiErr)
	}

	return nil
}

// Size возвращает суммарный размер всех ридеров.
func (m *MultiReader) Size() int64 {
	return m.totalSize
}
