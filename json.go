package main

import (
	"encoding/json"
	"io"
	"log"
	"strings"
)

// JSONIn provides methods of the Input interface
type JSONIn struct {
	reader   *json.Decoder
	firstRow []string
	name     []string
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

// GetColumn is read input to determine column of table
func (jr *JSONIn) GetColumn() ([]string, error) {
	var top interface{}
	err := jr.reader.Decode(&top)
	if err != nil {
		return nil, err
	}
	jr.name, jr.firstRow = jr.topLevel(top)
	debug.Printf("Column Name: [%v]", strings.Join(jr.name, ","))
	return jr.name, err
}

func (jr *JSONIn) topLevel(top interface{}) ([]string, []string) {
	switch top.(type) {
	case []interface{}:
		jr.ajson = top.([]interface{})
		val := jr.ajson[0]
		return jr.secondLevel(top, val)
	case map[string]interface{}:
		// {"a":"b"} object
		jr.ajson = nil
		return jr.objectFirstRow(top.(map[string]interface{}))
	default:
		log.Printf("Not a table format")
	}
	return nil, nil
}

// Analyze second when top is array
func (jr *JSONIn) secondLevel(top interface{}, second interface{}) ([]string, []string) {
	switch second.(type) {
	case map[string]interface{}:
		// [{}]
		return jr.objectFirstRow(second.(map[string]interface{}))
	case []interface{}:
		// [[]]
		return jr.etcFirstRow(second)
	default:
		// ["a","b"]
		jr.ajson = nil
		return jr.etcFirstRow(top)
	}
}

func (jr *JSONIn) objectFirstRow(obj map[string]interface{}) ([]string, []string) {
	// {"a":"b"} object
	var name []string
	var firstRow []string
	for k, v := range obj {
		name = append(name, k)
		firstRow = append(firstRow, jsonString(v))
	}
	return name, firstRow
}

func (jr *JSONIn) etcFirstRow(val interface{}) ([]string, []string) {
	// ex. array array
	// [["a"],
	//  ["b"]]
	var name []string
	var firstRow []string
	name = append(name, "c1")
	firstRow = append(firstRow, jsonString(val))
	return name, firstRow
}

func jsonString(val interface{}) string {
	switch val.(type) {
	case map[string]interface{}, []interface{}:
		str, err := json.Marshal(val)
		if err != nil {
			log.Printf("ERROR: jsonString:%s", err)
		}
		return valString(str)
	default:
		return valString(val)
	}
}

// FirstRowRead is read the first row
func (jr *JSONIn) FirstRowRead(list []interface{}) []interface{} {
	for i := range jr.name {
		list[i] = jr.firstRow[i]
	}
	return list
}

// RowRead is read 2row or later
func (jr *JSONIn) RowRead(list []interface{}) ([]interface{}, error) {
	if jr.ajson != nil {
		// [] array
		jr.count++
		if jr.count >= len(jr.ajson) {
			var top interface{}
			err := jr.reader.Decode(&top)
			if err != nil {
				return nil, err
			}
			_, jr.firstRow = jr.topLevel(top)
			jr.count = 0
		}
		if len(jr.ajson) > 0 {
			list = jr.rowParse(list, jr.ajson[jr.count])
		}
	} else {
		// {} object
		var data interface{}
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
		for i := range jr.name {
			list[i] = jsonString(dmap[jr.name[i]])
		}
	default:
		for i := range jr.name {
			list[i] = nil
		}
		list[0] = jsonString(jsonRow)
	}
	return list
}

func (trdsql *TRDSQL) jsonOutNew() Output {
	js := &JSONOut{}
	js.writer = json.NewEncoder(trdsql.outStream)
	js.writer.SetIndent("", "  ")
	return js
}

// First is preparation
func (js *JSONOut) First(columns []string) error {
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
