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

func (js *JSONOut) first(columns []string) error {
	js.results = make([]map[string]string, 0)
	return nil
}

func (js *JSONOut) rowWrite(values []interface{}, columns []string) error {
	m := make(map[string]string, len(columns))
	for i, col := range values {
		m[columns[i]] = valString(col)
	}
	js.results = append(js.results, m)
	return nil
}

func (js *JSONOut) last() {
	js.writer.Encode(js.results)
}
