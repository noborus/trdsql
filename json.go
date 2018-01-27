package main

import (
	"encoding/json"
	"io"
	"strings"
)

// JSONIn provides methods of the Input interface
type JSONIn struct {
	reader   *json.Decoder
	firstRow []string
	header   []string
	ajson    []interface{}
	count    int
}

// JSONOut provides methods of the Output interface
type JSONOut struct {
	writer  *json.Encoder
	results []map[string]string
}

func (trdsql *TRDSQL) jsonInputNew(r io.Reader) (Input, error) {
	jr := &JSONIn{}
	jr.reader = json.NewDecoder(r)
	return jr, nil
}

func (jr *JSONIn) firstRead() ([]string, error) {
	var data interface{}
	var dmap map[string]interface{}
	err := jr.reader.Decode(&data)
	if err != nil {
		return nil, err
	}
	switch data.(type) {
	case []interface{}:
		jr.ajson = data.([]interface{})
		jr.count = 0
		kv := jr.ajson[jr.count]
		dmap = kv.(map[string]interface{})
	case map[string]interface{}:
		jr.ajson = nil
		dmap = data.(map[string]interface{})
	}
	for k, v := range dmap {
		jr.header = append(jr.header, k)
		jr.firstRow = append(jr.firstRow, jsonStr(v))
	}
	debug.Printf("Column Name: [%v]", strings.Join(jr.header, ","))
	return jr.header, err
}

func jsonStr(val interface{}) string {
	switch val.(type) {
	case map[string]interface{}:
		str, _ := json.Marshal(val)
		return valString(str)
	case []interface{}:
		str, _ := json.Marshal(val)
		return valString(str)
	default:
		return valString(val)
	}
}

func (jr *JSONIn) firstRowRead(list []interface{}) []interface{} {
	for i := range jr.header {
		list[i] = jr.firstRow[i]
	}
	return list
}

func (jr *JSONIn) rowRead(list []interface{}) ([]interface{}, error) {
	var dmap map[string]interface{}
	if jr.ajson == nil {
		var data interface{}
		err := jr.reader.Decode(&data)
		if err != nil {
			return nil, err
		}
		dmap = data.(map[string]interface{})
	} else {
		jr.count++
		if len(jr.ajson) <= jr.count {
			return nil, io.EOF
		}
		kv := jr.ajson[jr.count]
		dmap = kv.(map[string]interface{})
	}
	for i := range jr.header {
		list[i] = jsonStr(dmap[jr.header[i]])
	}
	return list, nil
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

func (js *JSONOut) last() error {
	return js.writer.Encode(js.results)
}
