package trdsql

import (
	"encoding/hex"
	"encoding/json"
	"unicode/utf8"
)

// JSONOut provides methods of the Output interface
type JSONOut struct {
	writer  *json.Encoder
	results []map[string]interface{}
}

func (trdsql *TRDSQL) jsonOutNew() Output {
	js := &JSONOut{}
	js.writer = json.NewEncoder(trdsql.OutStream)
	js.writer.SetIndent("", "  ")
	return js
}

// First is preparation
func (js *JSONOut) First(columns []string, types []string) error {
	js.results = make([]map[string]interface{}, 0)
	return nil
}

// RowWrite is Addition to array
func (js *JSONOut) RowWrite(values []interface{}, columns []string) error {
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
		} else {
			return `\x` + hex.EncodeToString(t)
		}
	default:
		return v
	}
}

// Last is Actual output
func (js *JSONOut) Last() error {
	return js.writer.Encode(js.results)
}
