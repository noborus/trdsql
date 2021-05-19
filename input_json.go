package trdsql

// Convert JSON to a table.
// Supports the following JSON container types.
// * Array ([{c1: 1}, {c1: 2}, {c1: 3}])
// * Multiple JSON ({c1: 1}\n {c1: 2}\n {c1: 3}\n)

// Make a table from json and path.
import (
	"encoding/json"
	"errors"
	"io"
	"log"

	"github.com/itchyny/gojq"
)

// JSONReader provides methods of the Reader interface.
type JSONReader struct {
	reader  *json.Decoder
	preRead []map[string]string
	path    string
	names   []string
	types   []string
}

// NewJSONReader returns JSONReader and error.
func NewJSONReader(reader io.Reader, opts *ReadOpts) (*JSONReader, error) {
	r := &JSONReader{}
	r.reader = json.NewDecoder(reader)
	r.reader.UseNumber()
	r.path = opts.InPath
	var top interface{}

	for i := 0; i < opts.InPreRead; i++ {
		err := r.reader.Decode(&top)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return r, err
			}
			debug.Printf(err.Error())
			return r, nil
		}

		if r.path == "" {
			r, err = r.readAhead(top)
			if err != nil {
				return nil, err
			}
		} else {
			jquery, err := gojq.Parse(r.path)
			if err != nil {
				return nil, err
			}
			iter := jquery.Run(top)
			for {
				v, ok := iter.Next()
				if !ok {
					break
				}
				if err, ok := v.(error); ok {
					return r, err
				}
				r, err = r.readAhead(v)
				if err != nil {
					return r, err
				}
			}
		}
	}

	return r, nil
}

// Names returns column names.
func (r *JSONReader) Names() ([]string, error) {
	return r.names, nil
}

// Types returns column types.
// All JSON types return the DefaultDBType.
func (r *JSONReader) Types() ([]string, error) {
	r.types = make([]string, len(r.names))
	for i := 0; i < len(r.names); i++ {
		r.types[i] = DefaultDBType
	}
	return r.types, nil
}

func (r *JSONReader) readAhead(top interface{}) (*JSONReader, error) {
	switch m := top.(type) {
	case []interface{}:
		// []
		r.preRead = make([]map[string]string, 0, len(m))
		if r.reader.More() {
			pre, names, err := etcRow(m)
			if err != nil {
				return nil, err
			}

			r.names = names
			r.preRead = append(r.preRead, pre)
			return r, nil
		}

		already := map[string]bool{}
		for _, v := range m {
			pre, names, err := topLevel(v)
			if err != nil {
				return nil, err
			}

			for k := 0; k < len(names); k++ {
				if !already[names[k]] {
					already[names[k]] = true
					r.names = append(r.names, names[k])
				}
			}

			r.preRead = append(r.preRead, pre)
		}
		return r, nil
	default:
		pre, names, err := topLevel(m)
		if err != nil {
			return nil, err
		}
		r.names = names
		r.preRead = append(r.preRead, pre)
	}
	return r, nil
}

func topLevel(top interface{}) (map[string]string, []string, error) {
	switch obj := top.(type) {
	case map[string]interface{}:
		return objectRow(obj)
	default:
		return etcRow(obj)
	}
}

// PreReadRow is returns only columns that store preread rows.
// One json (not jsonl) returns all rows with preRead.
func (r *JSONReader) PreReadRow() [][]interface{} {
	rows := make([][]interface{}, len(r.preRead))
	for n, v := range r.preRead {
		rows[n] = make([]interface{}, len(r.names))
		for i := range r.names {
			rows[n][i] = v[r.names[i]]
		}
	}
	return rows
}

// ReadRow is read the rest of the row.
// Only jsonl requires ReadRow in json.
func (r *JSONReader) ReadRow(row []interface{}) ([]interface{}, error) {
	var data interface{}
	err := r.reader.Decode(&data)
	if err != nil {
		return nil, err
	}

	if r.path == "" {
		row = r.rowParse(row, data)
		return row, nil
	}

	// json query.
	jquery, err := gojq.Parse(r.path)
	if err != nil {
		return nil, err
	}
	iter := jquery.Run(data)
	debug.Printf(jquery.String())
	for {
		data, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := data.(error); ok {
			return nil, err
		}
		row = r.rowParse(row, data)
	}
	return row, nil
}

func (r *JSONReader) rowParse(row []interface{}, jsonRow interface{}) []interface{} {
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

func objectRow(obj map[string]interface{}) (map[string]string, []string, error) {
	// {"a":"b"} object
	names := make([]string, 0, len(obj))
	row := make(map[string]string)
	for k, v := range obj {
		names = append(names, k)
		row[k] = jsonString(v)
	}
	return row, names, nil
}

func etcRow(val interface{}) (map[string]string, []string, error) {
	// ex. array array
	// [["a"],
	//  ["b"]]
	var names []string
	k := "c1"
	names = append(names, k)
	row := make(map[string]string)
	row[k] = jsonString(val)
	return row, names, nil
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
