package main

import (
	"encoding/json"
	"io"
	"log"
	"strings"
)

// JSONIn provides methods of the Input interface
type JSONIn struct {
	reader  *json.Decoder
	preRead [][]string
	names   []string
	ajson   []interface{}
	count   int
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
func (jr *JSONIn) GetColumn(rowNum int) ([]string, error) {
	var top interface{}
	err := jr.reader.Decode(&top)
	if err != nil {
		return nil, err
	}
	jr.preRead = make([][]string, 1)
	jr.names, jr.preRead[0] = jr.topLevel(top)
	debug.Printf("Column Names: [%v]", strings.Join(jr.names, ","))
	return jr.names, err
}

func (jr *JSONIn) topLevel(top interface{}) ([]string, []string) {
	switch top.(type) {
	case []interface{}:
		// [{} or [] or etc...]
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

// PreReadRow is read the first row
func (jr *JSONIn) PreReadRow() [][]interface{} {
	rowNum := len(jr.preRead)
	rows := make([][]interface{}, rowNum)
	for n := 0; n < rowNum; n++ {
		rows[n] = make([]interface{}, len(jr.names))
		for i := range jr.names {
			rows[n][i] = jr.preRead[n][i]
		}
	}
	return rows
}

// ReadRow is read 2row or later
func (jr *JSONIn) ReadRow(row []interface{}) ([]interface{}, error) {
	if jr.ajson != nil {
		// [] array
		jr.count++
		if jr.count >= len(jr.ajson) {
			var top interface{}
			err := jr.reader.Decode(&top)
			if err != nil {
				return nil, err
			}
			jr.count = 0
		}
		if len(jr.ajson) > 0 {
			row = jr.rowParse(row, jr.ajson[jr.count])
		}
	} else {
		// {} object
		var data interface{}
		err := jr.reader.Decode(&data)
		if err != nil {
			return nil, err
		}
		row = jr.rowParse(row, data)
	}
	return row, nil
}

func (jr *JSONIn) rowParse(row []interface{}, jsonRow interface{}) []interface{} {
	switch jsonRow.(type) {
	case map[string]interface{}:
		dmap := jsonRow.(map[string]interface{})
		for i := range jr.names {
			row[i] = jsonString(dmap[jr.names[i]])
		}
	default:
		for i := range jr.names {
			row[i] = nil
		}
		row[0] = jsonString(jsonRow)
	}
	return row
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
