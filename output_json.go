package trdsql

import (
	"encoding/hex"
	"encoding/json"
	"unicode/utf8"
)

// JSONWriter provides methods of the Writer interface.
type JSONWriter struct {
	writer  *json.Encoder
	results []map[string]interface{}
}

// NewJSONWriter returns JSONWriter.
func NewJSONWriter(writeOpts *WriteOpts) *JSONWriter {
	w := &JSONWriter{}
	w.writer = json.NewEncoder(writeOpts.OutStream)
	w.writer.SetIndent("", "  ")
	return w
}

// PreWrite is area preparation.
func (w *JSONWriter) PreWrite(columns []string, types []string) error {
	w.results = make([]map[string]interface{}, 0)
	return nil
}

// WriteRow is Addition to array.
func (w *JSONWriter) WriteRow(values []interface{}, columns []string) error {
	m := make(map[string]interface{}, len(columns))
	for i, col := range values {
		m[columns[i]] = compatibleJSON(col)
	}
	w.results = append(w.results, m)
	return nil
}

// CompatibleJSON converts the value to a JSON-compatible value.
func compatibleJSON(v interface{}) interface{} {
	switch t := v.(type) {
	case []byte:
		j, err := toJSON(t)
		if err == nil {
			return j
		}
		if ok := utf8.Valid(t); ok {
			return string(t)
		}
		return `\x` + hex.EncodeToString(t)
	case string:
		j, err := toJSON([]byte(t))
		if err == nil {
			return j
		}
		return v
	default:
		return v
	}
}

func toJSON(s []byte) (json.RawMessage, error) {
	var js map[string]interface{}
	err := json.Unmarshal(s, &js)
	if err != nil {
		return s, err
	}
	return json.RawMessage(s), nil
}

// PostWrite is Actual output.
func (w *JSONWriter) PostWrite() error {
	return w.writer.Encode(w.results)
}
