/*
	Необходимо реализовать структуру MultiReader, которая объединяет
	несколько объектов, реализующих интерфейс SizedReadSeekCloser.
	Структура MultiReader должна сама реализовывать интерфейс SizedReadSeekCloser.

	Операции, которые должен поддерживать MultiReader:
		- Read: должен читать данные последовательно из всех переданных ридеров в том
				же порядке, в котором они переданы в NewMultiReader
		- Seek: должен позволять перемещать курсор на заданную позицию в объединенной
				последовательности ридеров.
		- Close: должен закрыть все ридеры.
		- Size: должен возвращать суммарный размер данных всех ридеров.
*/

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
