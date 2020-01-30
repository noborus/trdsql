package trdsql

import "errors"

type errorWriter struct{}

func (e errorWriter) PreWrite([]string, []string) error {
	return nil
}
func (e errorWriter) WriteRow([]interface{}, []string) error {
	return errors.New("Test")
}
func (e errorWriter) PostWrite() error {
	return nil
}
