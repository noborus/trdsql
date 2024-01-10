package trdsql

import (
	"encoding/hex"
	"encoding/json"
	"unicode/utf8"

	"github.com/iancoleman/orderedmap"
)

// JSONWriter provides methods of the Writer interface.
type JSONWriter struct {
	writer   *json.Encoder
	outNULL  string
	results  []*orderedmap.OrderedMap
	needNULL bool
}

// NewJSONWriter returns JSONWriter.
func NewJSONWriter(writeOpts *WriteOpts) *JSONWriter {
	w := &JSONWriter{}
	w.writer = json.NewEncoder(writeOpts.OutStream)
	w.writer.SetIndent("", "  ")
	w.needNULL = writeOpts.OutNeedNULL
	w.outNULL = writeOpts.OutNULL
	return w
}

// PreWrite is area preparation.
func (w *JSONWriter) PreWrite(columns []string, types []string) error {
	w.results = make([]*orderedmap.OrderedMap, 0)
	return nil
}

// WriteRow is Addition to array.
func (w *JSONWriter) WriteRow(values []any, columns []string) error {
	m := orderedmap.New()
	for i, col := range values {
		m.Set(columns[i], compatibleJSON(col, w.needNULL, w.outNULL))
	}
	w.results = append(w.results, m)
	return nil
}

// CompatibleJSON converts the value to a JSON-compatible value.
func compatibleJSON(v any, needNULL bool, outNULL string) any {
	switch t := v.(type) {
	case []byte:
		if isJSON(t) {
			return json.RawMessage(t)
		}
		if ok := utf8.Valid(t); ok {
			return string(t)
		}
		return `\x` + hex.EncodeToString(t)
	case string:
		if isJSON([]byte(t)) {
			return json.RawMessage(t)
		}
		return v
	default:
		if needNULL {
			return outNULL
		}
		return v
	}
}

// isJSON returns true if the byte array is JSON.
func isJSON(s []byte) bool {
	if len(s) == 0 {
		return false
	}

	// Except for JSONArray or JSONObject
	if s[0] != '[' && s[0] != '{' {
		return false
	}

	var js any
	err := json.Unmarshal(s, &js)
	return err == nil
}

// PostWrite is Actual output.
func (w *JSONWriter) PostWrite() error {
	return w.writer.Encode(w.results)
}
