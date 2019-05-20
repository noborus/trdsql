package main

import (
	"encoding/json"
)

// JSONOut provides methods of the Output interface
type JSONOut struct {
	writer  *json.Encoder
	results []map[string]string
}

func (trdsql *TRDSQL) jsonOutNew() Output {
	js := &JSONOut{}
	js.writer = json.NewEncoder(trdsql.outStream)
	js.writer.SetIndent("", "  ")
	return js
}

// First is preparation
func (js *JSONOut) First(columns []string, types []string) error {
	js.results = make([]map[string]string, 0)
	return nil
}

// RowWrite is Addition to array
func (js *JSONOut) RowWrite(values []interface{}, columns []string) error {
	m := make(map[string]string, len(columns))
	for i, col := range values {
		m[columns[i]] = valString(col)
	}
	js.results = append(js.results, m)
	return nil
}

// Last is Actual output
func (js *JSONOut) Last() error {
	return js.writer.Encode(js.results)
}
