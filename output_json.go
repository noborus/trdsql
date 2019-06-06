package trdsql

import (
	"encoding/hex"
	"encoding/json"
	"unicode/utf8"
)

// JSONWrite provides methods of the Output interface
type JSONWrite struct {
	writer  *json.Encoder
	results []map[string]interface{}
}

func NewJSONWrite() *JSONWrite {
	js := &JSONWrite{}
	js.writer = json.NewEncoder(DefaultWriteOpts.OutStream)
	js.writer.SetIndent("", "  ")
	return js
}

// First is preparation
func (js *JSONWrite) First(columns []string, types []string) error {
	js.results = make([]map[string]interface{}, 0)
	return nil
}

// WriteRow is Addition to array
func (js *JSONWrite) WriteRow(values []interface{}, columns []string) error {
	m := make(map[string]interface{}, len(columns))
	for i, col := range values {
		m[columns[i]] = valInterface(col)
	}
	js.results = append(js.results, m)
	return nil
}

func valInterface(v interface{}) interface{} {
	switch t := v.(type) {
	case []byte:
		if ok := utf8.Valid(t); ok {
			return string(t)
		}
		return `\x` + hex.EncodeToString(t)
	default:
		return v
	}
}

// Last is Actual output
func (js *JSONWrite) Last() error {
	return js.writer.Encode(js.results)
}
