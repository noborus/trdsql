package trdsql

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
)

// JSONRead provides methods of the Reader interface
type JSONRead struct {
	reader  *json.Decoder
	preRead []map[string]string
	names   []string
	types   []string
	inArray []interface{}
	count   int
}

func NewJSONReader(reader io.Reader) (Reader, error) {
	r := &JSONRead{}
	r.reader = json.NewDecoder(reader)
	return r, nil
}

// Convert JSON to a table.
// Supports the following JSON container types.
// * Array ([{c1: 1}, {c1: 2}, {c1: 3}])
// * Multiple JSON ({c1: 1}\n {c1: 2}\n {c1: 3}\n)

// GetColumn is reads the specified number of rows and determines the column name.
// The previously read row is stored in preRead.
func (r *JSONRead) GetColumn(rowNum int) ([]string, error) {
	var top interface{}
	names := map[string]bool{}
	for i := 0; i < rowNum; i++ {
		row, keys, err := r.readAhead(top, i)
		if err != nil {
			return r.names, err
		}
		r.preRead = append(r.preRead, row)
		for k := 0; k < len(keys); k++ {
			if !names[keys[k]] {
				names[keys[k]] = true
				r.names = append(r.names, keys[k])
			}
		}
	}
	return r.names, nil
}

// GetTypes is reads the specified number of rows and determines the column type.
func (r *JSONRead) GetTypes() ([]string, error) {
	r.types = make([]string, len(r.names))
	for i := 0; i < len(r.names); i++ {
		r.types[i] = DefaultDBType
	}
	return r.types, nil
}

func (r *JSONRead) readAhead(top interface{}, count int) (map[string]string, []string, error) {
	if r.inArray != nil {
		if len(r.inArray) > count {
			r.count++
			return r.secondLevel(top, r.inArray[count])
		}
		return nil, nil, io.EOF
	}
	err := r.reader.Decode(&top)
	if err != nil {
		return nil, nil, err
	}
	return r.topLevel(top)
}

func (r *JSONRead) topLevel(top interface{}) (map[string]string, []string, error) {
	switch obj := top.(type) {
	case []interface{}:
		// [{} or [] or etc...]
		r.inArray = obj
		return r.secondLevel(top, r.inArray[0])
	case map[string]interface{}:
		// {"a":"b"} object
		r.inArray = nil
		return r.objectFirstRow(obj)
	}
	return nil, nil, fmt.Errorf("JSON format could not be converted")
}

// Analyze second when top is array
func (r *JSONRead) secondLevel(top interface{}, second interface{}) (map[string]string, []string, error) {
	switch obj := second.(type) {
	case map[string]interface{}:
		// [{}]
		return r.objectFirstRow(obj)
	case []interface{}:
		// [[]]
		return r.etcFirstRow(second)
	default:
		// ["a","b"]
		r.inArray = nil
		return r.etcFirstRow(top)
	}
}

func (r *JSONRead) objectFirstRow(obj map[string]interface{}) (map[string]string, []string, error) {
	// {"a":"b"} object
	name := make([]string, 0, len(obj))
	row := make(map[string]string)
	for k, v := range obj {
		name = append(name, k)
		row[k] = jsonString(v)
	}
	return row, name, nil
}

func (r *JSONRead) etcFirstRow(val interface{}) (map[string]string, []string, error) {
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
		return ValString(str)
	default:
		return ValString(val)
	}
}

// PreReadRow is returns only columns that store preread rows.
func (r *JSONRead) PreReadRow() [][]interface{} {
	rowNum := len(r.preRead)
	rows := make([][]interface{}, rowNum)
	for n := 0; n < rowNum; n++ {
		rows[n] = make([]interface{}, len(r.names))
		for i := range r.names {
			rows[n][i] = r.preRead[n][r.names[i]]
		}
	}
	return rows
}

// ReadRow is read the rest of the row.
func (r *JSONRead) ReadRow(row []interface{}) ([]interface{}, error) {
	if r.inArray != nil {
		// [] array
		r.count++
		if r.count >= len(r.inArray) {
			var top interface{}
			err := r.reader.Decode(&top)
			if err != nil {
				return nil, err
			}
			r.count = 0
		}
		if len(r.inArray) > 0 {
			row = r.rowParse(row, r.inArray[r.count])
		}
	} else {
		// {} object
		var data interface{}
		err := r.reader.Decode(&data)
		if err != nil {
			return nil, fmt.Errorf("json format error:%s", err)
		}
		row = r.rowParse(row, data)
	}
	return row, nil
}

func (r *JSONRead) rowParse(row []interface{}, jsonRow interface{}) []interface{} {
	switch m := jsonRow.(type) {
	case map[string]interface{}:
		for i := range r.names {
			row[i] = jsonString(m[r.names[i]])
		}
	default:
		for i := range r.names {
			row[i] = nil
		}
		row[0] = jsonString(jsonRow)
	}
	return row
}
