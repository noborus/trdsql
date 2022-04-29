package trdsql

import (
	"encoding/json"

	"github.com/iancoleman/orderedmap"
)

// JSONLWriter provides methods of the Writer interface.
type JSONLWriter struct {
	writer   *json.Encoder
	needNULL bool
	outNULL  string
}

// NewJSONLWriter returns JSONLWriter.
func NewJSONLWriter(writeOpts *WriteOpts) *JSONLWriter {
	w := &JSONLWriter{}
	w.writer = json.NewEncoder(writeOpts.OutStream)
	w.needNULL = writeOpts.OutNeedNULL
	w.outNULL = writeOpts.OutNULL
	return w
}

// PreWrite does nothing.
func (w *JSONLWriter) PreWrite(columns []string, types []string) error {
	return nil
}

// WriteRow is write one JSONL.
func (w *JSONLWriter) WriteRow(values []interface{}, columns []string) error {
	m := orderedmap.New()
	for i, col := range values {
		m.Set(columns[i], compatibleJSON(col, w.needNULL, w.outNULL))
	}
	return w.writer.Encode(m)
}

// PostWrite does nothing.
func (w *JSONLWriter) PostWrite() error {
	return nil
}
