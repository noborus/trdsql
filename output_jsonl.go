package trdsql

import (
	"encoding/json"
)

// JSONLWriter provides methods of the Writer interface.
type JSONLWriter struct {
	writer *json.Encoder
}

// NewJSONLWriter returns JSONLWriter.
func NewJSONLWriter(writeOpts *WriteOpts) *JSONLWriter {
	w := &JSONLWriter{}
	w.writer = json.NewEncoder(writeOpts.OutStream)
	return w
}

// PreWrite does nothing.
func (w *JSONLWriter) PreWrite(columns []string, types []string) error {
	return nil
}

// WriteRow is write one JSONL.
func (w *JSONLWriter) WriteRow(values []interface{}, columns []string) error {
	m := make(map[string]interface{}, len(columns))
	for i, col := range values {
		m[columns[i]] = compatibleJSON(col)
	}
	return w.writer.Encode(m)
}

// PostWrite does nothing.
func (w *JSONLWriter) PostWrite() error {
	return nil
}
