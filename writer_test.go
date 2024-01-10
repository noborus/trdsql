package trdsql

import "errors"

// ErrTest returns a test error.
var ErrTest = errors.New("Test")

type errorWriter struct{}

func (e errorWriter) PreWrite([]string, []string) error {
	return nil
}

func (e errorWriter) WriteRow([]any, []string) error {
	return ErrTest
}

func (e errorWriter) PostWrite() error {
	return nil
}
