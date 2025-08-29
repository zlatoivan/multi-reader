package main

import (
	"errors"
	"io"
)

// TestCase описывает один самостоятельный тест: имя и функцию проверки.
type TestCase struct {
	name string
	run  func() bool
}

var testCases = []TestCase{
	{
		name: "Size и последовательное чтение",
		run: func() bool {
			a := newMockStringsReader("abc")
			b := newMockStringsReader("defg")
			m := NewMultiReader(a, b)

			if m.Size() != int64(7) {
				return false
			}

			buf := make([]byte, 7)
			n, err := m.Read(buf)
			if err != nil {
				return false
			}
			if n != 7 {
				return false
			}
			return string(buf) == "abcdefg"
		},
	},
	{
		name: "Поведение EOF",
		run: func() bool {
			a := newMockStringsReader("hi")
			m := NewMultiReader(a)
			buf := make([]byte, 2)

			n, err := m.Read(buf)
			if err != nil || n != 2 || string(buf) != "hi" {
				return false
			}

			n, err = m.Read(buf)
			if n != 0 {
				return false
			}
			return errors.Is(err, io.EOF)
		},
	},
	{
		name: "Seek от начала и чтение",
		run: func() bool {
			a := newMockStringsReader("hello")
			b := newMockStringsReader("-world-")
			m := NewMultiReader(a, b)

			pos, err := m.Seek(3, io.SeekStart)
			if err != nil || pos != 3 {
				return false
			}

			buf := make([]byte, 5)
			n, err := m.Read(buf)
			if err != nil {
				return false
			}
			return string(buf[:n]) == "lo-wo"
		},
	},
}
