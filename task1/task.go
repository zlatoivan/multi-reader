//go:build task_template

/*
type io.ReadSeekCloser interface {
	Read(p []byte) (n int, err error)
	Seek(offset int64, whence int) (int64, error)
	Close() error
}
*/

package main

import "io"

type SizedReadSeekCloser interface {
	io.ReadSeekCloser
	Size() int64
}

type MultiReader struct {
	// put your code here...
}

func NewMultiReader(rs ...SizedReadSeekCloser) *MultiReader {
	// put your code here...
	return nil
}
