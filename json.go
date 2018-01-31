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
	var top interface{}
	err := jr.reader.Decode(&top)
	if err != nil {
		return nil, err
	}
	err = jr.topRow(top)
	if err != nil {
		return nil, err
	}
	debug.Printf("Column Name: [%v]", strings.Join(jr.header, ","))
	return jr.header, err
}

func (jr *JSONIn) topRow(top interface{}) error {
	switch top.(type) {
	case []interface{}:
		jr.ajson = top.([]interface{})
		val := jr.ajson[0]
		switch val.(type) {
		case map[string]interface{}:
			jr.objectFirstRow(val.(map[string]interface{}))
			return nil
		case []interface{}:
			jr.etcFirstRow(val)
		default:
			// ["a","b"]
			jr.ajson = nil
			jr.etcFirstRow(top)
		}
	case map[string]interface{}:
		// {"a":"b"} object
		jr.ajson = nil
		jr.objectFirstRow(top.(map[string]interface{}))
	}
	return nil
}

func (jr *JSONIn) objectFirstRow(obj map[string]interface{}) {
	// {"a":"b"} object
	for k, v := range obj {
		jr.header = append(jr.header, k)
		jr.firstRow = append(jr.firstRow, jsonStr(v))
	}
}

func (jr *JSONIn) etcFirstRow(val interface{}) {
	// array array
	// [["a"],
	//  ["b"]]
	jr.header = append(jr.header, "c1")
	jr.firstRow = append(jr.firstRow, jsonStr(val))
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
	if jr.ajson != nil {
		// [] array
		jr.count++
		if jr.count >= len(jr.ajson) {
			return nil, io.EOF
		}
		list = jr.rowParse(list, jr.ajson[jr.count])
	} else {
		var data interface{}
		// {} object
		err := jr.reader.Decode(&data)
		if err != nil {
			return nil, err
		}
		list = jr.rowParse(list, data)
	}
	return list, nil
}

func (jr *JSONIn) rowParse(list []interface{}, jsonRow interface{}) []interface{} {
	switch jsonRow.(type) {
	case map[string]interface{}:
		dmap := jsonRow.(map[string]interface{})
		for i := range jr.header {
			list[i] = jsonStr(dmap[jr.header[i]])
		}
	default:
		list[0] = jsonStr(jsonRow)
	}
	return list
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
