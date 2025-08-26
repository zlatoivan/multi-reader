package main

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// closableStrings — адаптер поверх strings.Reader, реализующий SizedReadSeekCloser.
type closableStrings struct {
	*strings.Reader
	size      int64
	closed    bool
	closeErr  error
	sizeCalls *int
	seekCalls *int
}

func newClosableStrings(s string) *closableStrings {
	r := strings.NewReader(s)
	return &closableStrings{
		Reader: r,
		size:   int64(r.Len()),
	}
}

func (c *closableStrings) Read(p []byte) (int, error) {
	return c.Reader.Read(p)
}

func (c *closableStrings) Seek(offset int64, whence int) (int64, error) {
	if c.seekCalls != nil {
		*c.seekCalls++
	}
	return c.Reader.Seek(offset, whence)
}

func (c *closableStrings) Close() error {
	c.closed = true
	return c.closeErr
}

func (c *closableStrings) Size() int64 {
	if c.sizeCalls != nil {
		*c.sizeCalls++
	}
	return c.size
}

func TestNewMultiReader_SizeAndRead(t *testing.T) {
	t.Parallel()

	a := newClosableStrings("abc")
	b := newClosableStrings("defg")
	m := NewMultiReader(a, b)

	require.Equal(t, int64(7), m.Size())

	buf := make([]byte, 7)
	n, err := m.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 7, n)
	assert.Equal(t, "abcdefg", string(buf))
}

func TestRead_EOFBehavior(t *testing.T) {
	t.Parallel()

	a := newClosableStrings("hi")
	m := NewMultiReader(a)
	buf := make([]byte, 2)

	n, err := m.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	assert.Equal(t, "hi", string(buf))

	n, err = m.Read(buf)
	require.Equal(t, 0, n)
	require.ErrorIs(t, err, io.EOF)
}

func TestSeek_FromStartAndRead(t *testing.T) {
	t.Parallel()

	a := newClosableStrings("hello")
	b := newClosableStrings("-world-")
	m := NewMultiReader(a, b)

	pos, err := m.Seek(3, io.SeekStart) // expect to start at 'l' in "hello"
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)

	buf := make([]byte, 5)
	n, err := m.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "lo-wo", string(buf[:n]))
}

func TestSeek_FromEnd(t *testing.T) {
	t.Parallel()

	a := newClosableStrings("abc")
	b := newClosableStrings("def")
	m := NewMultiReader(a, b) // total 6

	pos, err := m.Seek(-2, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(4), pos)

	buf := make([]byte, 2)
	n, err := m.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	assert.Equal(t, "ef", string(buf))
}

func TestSeek_FromCurrent(t *testing.T) {
	t.Parallel()

	a := newClosableStrings("abcd")
	m := NewMultiReader(a)

	buf := make([]byte, 1)
	n, err := m.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assert.Equal(t, "a", string(buf))

	pos, err := m.Seek(2, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)

	n, err = m.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assert.Equal(t, "d", string(buf))
}

func TestSeek_Errors(t *testing.T) {
	t.Parallel()

	a := newClosableStrings("abc")
	m := NewMultiReader(a)

	_, err := m.Seek(0, 99)
	require.Error(t, err)

	_, err = m.Seek(-1, io.SeekStart)
	require.Error(t, err)

	_, err = m.Seek(5, io.SeekStart)
	require.Error(t, err)
}

func TestClose_AggregatesErrors(t *testing.T) {
	t.Parallel()

	errA := errors.New("A")
	errB := errors.New("B")
	a := newClosableStrings("x")
	b := newClosableStrings("y")
	c := newClosableStrings("z")
	a.closeErr = errA
	b.closeErr = errB

	m := NewMultiReader(a, b, c)

	err := m.Close()
	require.Error(t, err)
	require.ErrorIs(t, err, errA)
	require.ErrorIs(t, err, errB)
	assert.True(t, a.closed && b.closed && c.closed, "not all readers were closed")
}

func TestSize_IsCachedNotRecomputed(t *testing.T) {
	t.Parallel()

	var calls int
	tr1 := newClosableStrings(strings.Repeat("a", 2))
	tr2 := newClosableStrings(strings.Repeat("b", 3))
	tr1.sizeCalls = &calls
	tr2.sizeCalls = &calls

	m := NewMultiReader(tr1, tr2)
	require.Equal(t, 2, calls)
	_ = m.Size()
	_ = m.Size()
	require.Equal(t, 2, calls)
}

func TestLazySeek_PerformedOnRead(t *testing.T) {
	t.Parallel()

	var seekCalls1, seekCalls2 int
	tr1 := newClosableStrings("abc")
	tr2 := newClosableStrings("def")
	tr1.seekCalls = &seekCalls1
	tr2.seekCalls = &seekCalls2

	m := NewMultiReader(tr1, tr2)

	// Перейти в середину второго ридера, но не читать
	pos, err := m.Seek(4, io.SeekStart) // total index 4 -> 'e'
	require.NoError(t, err)
	require.Equal(t, int64(4), pos)
	assert.Equal(t, 0, seekCalls1)
	assert.Equal(t, 0, seekCalls2)

	buf := make([]byte, 1)
	n, err := m.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assert.Equal(t, "e", string(buf))
	assert.Equal(t, 0, seekCalls1)
	assert.Greater(t, seekCalls2, 0)
}
