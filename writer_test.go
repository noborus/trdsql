package trdsql

import "errors"

var (
	// ErrTest returns a test error.
	ErrTest = errors.New("Test")
)

type errorWriter struct{}

func (e errorWriter) PreWrite([]string, []string) error {
	return nil
}
func (e errorWriter) WriteRow([]interface{}, []string) error {
	return ErrTest
}
func (e errorWriter) PostWrite() error {
	return nil
}
