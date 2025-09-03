package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"
)

const concurrentTestTimeout = time.Second * 30

func AssertEqual[T comparable, IN any](message string, expected T, testFunc func(IN) T, input IN) {
	AssertEqualT[T, IN](message, expected, testFunc, input, compareSimpleTypes[T])
}

func AssertEqualValues[T comparable, IN any](message string, expected []T, testFunc func(IN) []T, input IN) {
	AssertEqualT[[]T, IN](message, expected, testFunc, input, compareSliceValues[T])
}

func AssertEqualT[T any, IN any](message string, expected T, testFunc func(IN) T, input IN, compare func(T, T) bool) {
	defer catchPanic(message)()

	actual := testFunc(input)

	if !compare(expected, actual) {
		_, _ = fmt.Fprintf(
			os.Stderr,
			"Тест кейс %q - провал\n\tОжидаемый результат - %v\n\tТекущий результат - %v\n\tВходные данные - %v\n",
			message,
			expected,
			actual,
			input,
		)
		os.Exit(1)
	}

	_, _ = fmt.Fprintf(os.Stderr, "Тест кейс %q - успех\n", message)
}

func AssertPanic(cb func()) (hasPanic bool) {
	defer func() {
		if err := recover(); err != nil {
			hasPanic = true
		}
	}()

	cb()

	return false
}

func CustomTestBody[T any](message string, prepare func() T, check func(T) bool) {
	defer catchPanic(message)()

	isSuccess := check(prepare())

	if !isSuccess {
		_, _ = fmt.Fprintf(
			os.Stderr,
			"Тест кейс %q - провал\n",
			message,
		)
		os.Exit(1)
	}

	_, _ = fmt.Fprintf(os.Stderr, "Тест кейс %q - успех\n", message)
}

func AssertPrint(message string, expected string, cb func()) {
	CustomTestBody(
		message,
		func() string { return catchPrint(cb) },
		func(actual string) bool {
			return actual == expected
		},
	)
}

func ConcurrentCustomTestBody[T any](message string, prepare func() T, check func(T) bool) {
	ctx, cancel := context.WithTimeout(context.Background(), concurrentTestTimeout)
	defer cancel()

	finished := make(chan struct{}, 1)

	go func() {
		CustomTestBody(message, prepare, check)
		finished <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		_, _ = fmt.Fprintf(
			os.Stderr,
			"Тест кейс %q - таймаут\n",
			message,
		)

		os.Exit(1)
	case <-finished:
	}
}

func compareSimpleTypes[T comparable](expected T, actual T) bool {
	return expected == actual
}

func compareSliceValues[T comparable](expected []T, actual []T) bool {
	if len(expected) != len(actual) {
		return false
	}

	for i := 0; i < len(expected); i++ {
		if expected[i] != actual[i] {
			return false
		}
	}

	return true
}

func catchPanic(message string) func() {
	return func() {
		if r := recover(); r != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Тест кейс %q - Паника: %s\n", message, r)
			os.Exit(1)
		}
	}
}

func catchPrint(cb func()) string {

	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	defer func() {
		os.Stdout = old // restoring the real stdout
	}()

	func() {
		cb()

		w.Close() // Close pipe
	}()

	caughtOutput := make(chan string)
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r) // Read until pipe will close

		caughtOutput <- buf.String()
	}()

	return <-caughtOutput
}

func ContainsAll(slice []string, values ...string) bool {
	if len(values) > len(slice) {
		return false
	}

	existValues := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		existValues[s] = struct{}{}
	}

	for _, value := range values {
		if _, ok := existValues[value]; !ok {
			return false
		}
	}

	return true
}
