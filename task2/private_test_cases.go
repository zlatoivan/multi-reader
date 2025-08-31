package main

import (
	"errors"
	"io"
	"strings"
)

var privateTestCases = []TestCase{
	{
		name: "Seek от конца",
		run: func() bool {
			a := newMockStringsReader("abc")
			b := newMockStringsReader("def")
			m := NewMultiReader(4, a, b)

			pos, err := m.Seek(-2, io.SeekEnd)
			if err != nil || pos != 4 {
				return false
			}

			buf := make([]byte, 2)
			n, err := m.Read(buf)
			if err != nil || n != 2 {
				return false
			}
			return string(buf) == "ef"
		},
	},
	{
		name: "Seek от текущей позиции",
		run: func() bool {
			a := newMockStringsReader("abcd")
			m := NewMultiReader(4, a)

			buf := make([]byte, 1)
			n, err := m.Read(buf)
			if err != nil || n != 1 || string(buf) != "a" {
				return false
			}

			pos, err := m.Seek(2, io.SeekCurrent)
			if err != nil || pos != 3 {
				return false
			}

			n, err = m.Read(buf)
			if err != nil || n != 1 {
				return false
			}
			return string(buf) == "d"
		},
	},
	{
		name: "Ошибочные варианты Seek",
		run: func() bool {
			a := newMockStringsReader("abc")
			m := NewMultiReader(4, a)

			if _, err := m.Seek(0, 99); err == nil {
				return false
			}
			if _, err := m.Seek(-1, io.SeekStart); err == nil {
				return false
			}
			if _, err := m.Seek(5, io.SeekStart); err == nil {
				return false
			}
			return true
		},
	},
	{
		name: "Close агрегирует ошибки",
		run: func() bool {
			errA := errors.New("A")
			errB := errors.New("B")
			a := newMockStringsReader("x")
			b := newMockStringsReader("y")
			c := newMockStringsReader("z")
			a.closeErr = errA
			b.closeErr = errB

			m := NewMultiReader(4, a, b, c)

			err := m.Close()
			if err == nil {
				return false
			}
			if !errors.Is(err, errA) || !errors.Is(err, errB) {
				return false
			}
			return a.closed && b.closed && c.closed
		},
	},
	{
		name: "Read/Seek после Close",
		run: func() bool {
			a := newMockStringsReader("abc")
			m := NewMultiReader(4, a)

			err := m.Close()
			if err != nil {
				return false
			}

			buf := make([]byte, 1)
			n, err := m.Read(buf)
			if n != 0 || !errors.Is(err, io.ErrClosedPipe) {
				return false
			}

			if _, err = m.Seek(0, io.SeekStart); !errors.Is(err, io.ErrClosedPipe) {
				return false
			}

			err = m.Close()
			return err == nil
		},
	},
	{
		name: "Size кэшируется и не пересчитывается",
		run: func() bool {
			var calls int
			tr1 := newMockStringsReader(strings.Repeat("a", 2))
			tr2 := newMockStringsReader(strings.Repeat("b", 3))
			tr1.sizeCalls = &calls
			tr2.sizeCalls = &calls

			m := NewMultiReader(4, tr1, tr2)
			if calls != 2 {
				return false
			}
			_ = m.Size()
			_ = m.Size()
			return calls == 2
		},
	},
	{
		name: "Ленивый Seek выполняется при первом чтении",
		run: func() bool {
			var seekCalls1, seekCalls2 int
			tr1 := newMockStringsReader("abc")
			tr2 := newMockStringsReader("def")
			tr1.seekCalls = &seekCalls1
			tr2.seekCalls = &seekCalls2

			m := NewMultiReader(4, tr1, tr2)

			pos, err := m.Seek(4, io.SeekStart)
			if err != nil || pos != 4 {
				return false
			}
			if seekCalls1 != 0 || seekCalls2 != 0 {
				return false
			}

			buf := make([]byte, 1)
			n, err := m.Read(buf)
			if err != nil || n != 1 || string(buf) != "e" {
				return false
			}
			if seekCalls1 != 0 {
				return false
			}
			return seekCalls2 > 0
		},
	},
	{
		name: "Seek на EOF допустим и Read возвращает EOF",
		run: func() bool {
			a := newMockStringsReader("data")
			m := NewMultiReader(4, a)

			size := m.Size()
			pos, err := m.Seek(0, io.SeekEnd)
			if err != nil || pos != size {
				return false
			}

			buf := make([]byte, 1)
			n, err := m.Read(buf)
			if n != 0 {
				return false
			}
			return errors.Is(err, io.EOF)
		},
	},
	{
		name: "Read с нулевой длиной возвращает (0, nil)",
		run: func() bool {
			a := newMockStringsReader("xy")
			m := NewMultiReader(4, a)
			n, err := m.Read(nil)
			return n == 0 && err == nil
		},
	},
	{
		name: "Seek внутри буферного окна не вызывает нижний Seek",
		run: func() bool {
			var seekCalls int
			a := newMockStringsReader("hello world")
			a.seekCalls = &seekCalls
			m := NewMultiReader(4, a)
			buf := make([]byte, 1)
			// Старт чтения, префетчер станет активным и сделает первый Seek
			if n, err := m.Read(buf); err != nil || n != 1 {
				return false
			}
			before := seekCalls
			// Переход вперёд на 1 байт — должен быть внутри уже буферизованного окна
			if _, err := m.Seek(1, io.SeekCurrent); err != nil {
				return false
			}
			// Следующее чтение должно прийти из буфера, без новых Seek в источнике
			if n, err := m.Read(buf); err != nil || n != 1 {
				return false
			}
			return seekCalls == before
		},
	},
	{
		name: "Seek назад за пределы окна инициирует новый нижний Seek",
		run: func() bool {
			var seekCalls int
			a := newMockStringsReader("longstringdata")
			a.seekCalls = &seekCalls
			m := NewMultiReader(4, a)
			buf := make([]byte, 5)
			if n, err := m.Read(buf); err != nil || n != 5 { // прочитаем немного вперёд
				return false
			}
			before := seekCalls
			// Сильно назад — за границы текущего окна (bufferStart уже сдвинут вперёд)
			if _, err := m.Seek(0, io.SeekStart); err != nil {
				return false
			}
			// Первое же чтение должно потребовать нижний Seek
			if n, err := m.Read(buf[:1]); err != nil || n != 1 {
				return false
			}
			return seekCalls > before
		},
	},
	{
		name: "Маленькие ридеры, большие чанки",
		run: func() bool {
			a := newMockStringsReader("aaaaa")
			b := newMockStringsReader("bbb")
			c := newMockStringsReader("cccccccc")
			m := NewMultiReader(2, a, b, c)
			buf := make([]byte, int(m.Size()))
			n, err := m.Read(buf)
			if err != nil || n != len(buf) {
				return false
			}
			return string(buf) == "aaaaabbbcccccccc"
		},
	},
	{
		name: "Seek назад внутри окна и сразу Read — буфер не сбрасывается",
		run: func() bool {
			var seeks int
			r := newMockStringsReader("abcdef")
			r.seekCalls = &seeks
			m := NewMultiReader(2, r)
			buf := make([]byte, 4)
			_, _ = m.Read(buf) // abcd
			before := seeks
			if _, err := m.Seek(-1, io.SeekCurrent); err != nil { // позиция на 'd'
				return false
			}
			b2 := make([]byte, 1)
			n, err := m.Read(b2)
			if err != nil || n != 1 || string(b2) != "d" {
				return false
			}
			return seeks == before
		},
	},
	{
		name: "Дальний Seek вперёд за окно и немедленный Read — новый префетч",
		run: func() bool {
			var seeks int
			r := newMockStringsReader(strings.Repeat("x", 64))
			r.seekCalls = &seeks
			m := NewMultiReader(2, r)
			buf := make([]byte, 8)
			_, _ = m.Read(buf) // прогреем окно
			before := seeks
			if _, err := m.Seek(50, io.SeekStart); err != nil {
				return false
			}
			b2 := make([]byte, 1)
			n, err := m.Read(b2)
			if err != nil || n != 1 || string(b2) != "x" {
				return false
			}
			return seeks > before
		},
	},
	{
		name: "EOF-контракт при n>0 и err==EOF из источника",
		run: func() bool {
			r := newMockStringsReader("z")
			m := NewMultiReader(1, r)
			b := make([]byte, 10)
			n, err := m.Read(b)
			if err != nil || n != 1 || string(b[:n]) != "z" {
				return false
			}
			n, err = m.Read(b)
			return n == 0 && errors.Is(err, io.EOF)
		},
	},
	{
		name: "Close во время фонового чтения не падает",
		run: func() bool {
			r := newMockStringsReader(strings.Repeat("a", 1<<16))
			m := NewMultiReader(2, r)
			done := make(chan struct{})
			go func() {
				buf := make([]byte, 1<<15)
				_, _ = m.Read(buf)
				close(done)
			}()
			_ = m.Close()
			<-done
			return true
		},
	},
	{
		name: "Большие данные: полное чтение и чтение через границы",
		run: func() bool {
			// Сгенерируем несколько «больших» источников по ~1–2KB суммарно
			s1 := strings.Repeat("A", 1024)
			s2 := strings.Repeat("B", 768)
			s3 := strings.Repeat("C", 512)
			a := newMockStringsReader(s1)
			b := newMockStringsReader(s2)
			c := newMockStringsReader(s3)
			m := NewMultiReader(4, a, b, c)

			// Полное чтение и сравнение
			expected := s1 + s2 + s3
			buf := make([]byte, len(expected))
			n, err := m.Read(buf)
			if err != nil || n != len(expected) || string(buf) != expected {
				return false
			}

			// Seek в конце первого ридера минус 10, прочитать 20 байт — пересекаем границу A->B
			if _, err := m.Seek(int64(len(s1)-10), io.SeekStart); err != nil {
				return false
			}
			buf2 := make([]byte, 20)
			n, err = m.Read(buf2)
			if err != nil || n != 20 {
				return false
			}
			if string(buf2) != strings.Repeat("A", 10)+strings.Repeat("B", 10) {
				return false
			}

			// Seek на конец второго ридера минус 5, прочитать 15 — пересекаем границу B->C
			offset := int64(len(s1) + len(s2) - 5)
			if _, err := m.Seek(offset, io.SeekStart); err != nil {
				return false
			}
			buf3 := make([]byte, 15)
			n, err = m.Read(buf3)
			if err != nil || n != 15 {
				return false
			}
			if string(buf3) != strings.Repeat("B", 5)+strings.Repeat("C", 10) {
				return false
			}
			return true
		},
	},
}
