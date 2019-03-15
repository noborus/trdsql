package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
)

// JSONIn provides methods of the Input interface
type JSONIn struct {
	reader  *json.Decoder
	preRead []map[string]string
	names   []string
	inArray []interface{}
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

// Convert JSON to a table.
// Supports the following JSON container types.
// * Array ([{c1: 1}, {c1: 2}, {c1: 3}])
// * Multiple JSON ({c1: 1}\n {c1: 2}\n {c1: 3}\n)

// GetColumn is reads the specified number of rows and determines the column name.
// The previously read row is stored in preRead.
func (jr *JSONIn) GetColumn(rowNum int) ([]string, error) {
	var top interface{}
	names := map[string]bool{}
	for i := 0; i < rowNum; i++ {
		row, keys, err := jr.readAhead(top, i)
		if err != nil {
			return jr.names, err
		}
		jr.preRead = append(jr.preRead, row)
		for k := 0; k < len(keys); k++ {
			if !names[keys[k]] {
				names[keys[k]] = true
				jr.names = append(jr.names, keys[k])
			}
		}
	}
	return jr.names, nil
}

func (jr *JSONIn) readAhead(top interface{}, rcount int) (map[string]string, []string, error) {
	if jr.inArray != nil {
		if len(jr.inArray) > rcount {
			jr.count++
			return jr.secondLevel(top, jr.inArray[rcount])
		}
		return nil, nil, io.EOF
	}
	err := jr.reader.Decode(&top)
	if err != nil {
		return nil, nil, err
	}
	return jr.topLevel(top)
}

func (jr *JSONIn) topLevel(top interface{}) (map[string]string, []string, error) {
	switch top.(type) {
	case []interface{}:
		// [{} or [] or etc...]
		jr.inArray = top.([]interface{})
		val := jr.inArray[0]
		return jr.secondLevel(top, val)
	case map[string]interface{}:
		// {"a":"b"} object
		jr.inArray = nil
		return jr.objectFirstRow(top.(map[string]interface{}))
	}
	return nil, nil, fmt.Errorf("JSON format could not be converted")
}

// Analyze second when top is array
func (jr *JSONIn) secondLevel(top interface{}, second interface{}) (map[string]string, []string, error) {
	switch second.(type) {
	case map[string]interface{}:
		// [{}]
		return jr.objectFirstRow(second.(map[string]interface{}))
	case []interface{}:
		// [[]]
		return jr.etcFirstRow(second)
	default:
		// ["a","b"]
		jr.inArray = nil
		return jr.etcFirstRow(top)
	}
}

func (jr *JSONIn) objectFirstRow(obj map[string]interface{}) (map[string]string, []string, error) {
	// {"a":"b"} object
	name := make([]string, 0, len(obj))
	row := make(map[string]string)
	for k, v := range obj {
		name = append(name, k)
		row[k] = jsonString(v)
	}
	return row, name, nil
}

func (jr *JSONIn) etcFirstRow(val interface{}) (map[string]string, []string, error) {
	// ex. array array
	// [["a"],
	//  ["b"]]
	var name []string
	k := "c1"
	name = append(name, k)
	row := make(map[string]string)
	row[k] = jsonString(val)
	return row, name, nil
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

// PreReadRow is returns only columns that store preread rows.
func (jr *JSONIn) PreReadRow() [][]interface{} {
	rowNum := len(jr.preRead)
	rows := make([][]interface{}, rowNum)
	for n := 0; n < rowNum; n++ {
		rows[n] = make([]interface{}, len(jr.names))
		for i := range jr.names {
			rows[n][i] = jr.preRead[n][jr.names[i]]
		}
	}
	return rows
}

// ReadRow is read the rest of the row.
func (jr *JSONIn) ReadRow(row []interface{}) ([]interface{}, error) {
	if jr.inArray != nil {
		// [] array
		jr.count++
		if jr.count >= len(jr.inArray) {
			var top interface{}
			err := jr.reader.Decode(&top)
			if err != nil {
				return nil, err
			}
			jr.count = 0
		}
		if len(jr.inArray) > 0 {
			row = jr.rowParse(row, jr.inArray[jr.count])
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
