package trdsql

import (
	yaml "github.com/goccy/go-yaml"
)

// YAMLWriter provides methods of the Writer interface.
type YAMLWriter struct {
	writer  *yaml.Encoder
	results []map[string]interface{}
}

// NewYAMLWriter returns YAMLWriter.
func NewYAMLWriter(writeOpts *WriteOpts) *YAMLWriter {
	w := &YAMLWriter{}
	w.writer = yaml.NewEncoder(writeOpts.OutStream)
	return w
}

// PreWrite is area preparation.
func (w *YAMLWriter) PreWrite(columns []string, types []string) error {
	w.results = make([]map[string]interface{}, 0)
	return nil
}

// WriteRow is Addition to array.
func (w *YAMLWriter) WriteRow(values []interface{}, columns []string) error {
	m := make(map[string]interface{})
	for i, col := range values {
		m[columns[i]] = col
	}
	w.results = append(w.results, m)
	return nil
}

// PostWrite is Actual output.
func (w *YAMLWriter) PostWrite() error {
	return w.writer.Encode(w.results)
}
