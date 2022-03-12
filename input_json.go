package trdsql

// Convert JSON to a table.
// Supports the following JSON container types.
// * Array ([{c1: 1}, {c1: 2}, {c1: 3}])
// * Multiple JSON ({c1: 1}\n {c1: 2}\n {c1: 3}\n)

// Make a table from json
// or make the result of json filter by jq.
import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/itchyny/gojq"
)

// JSONReader provides methods of the Reader interface.
type JSONReader struct {
	reader    *json.Decoder
	preRead   []map[string]string
	query     *gojq.Query
	already   map[string]bool
	names     []string
	types     []string
	limitRead bool
}

// NewJSONReader returns JSONReader and error.
func NewJSONReader(reader io.Reader, opts *ReadOpts) (*JSONReader, error) {
	r := &JSONReader{}
	r.reader = json.NewDecoder(reader)
	r.reader.UseNumber()
	r.already = make(map[string]bool)
	var top interface{}

	if opts.InJQuery != "" {
		str := trimQuoteAll(opts.InJQuery)
		query, err := gojq.Parse(str)
		if err != nil {
			return nil, fmt.Errorf("gojq: %w (%s)", err, opts.InJQuery)
		}
		r.query = query
	}

	r.limitRead = opts.InLimitRead

	for i := 0; i < opts.InPreRead; i++ {
		if err := r.reader.Decode(&top); err != nil {
			if !errors.Is(err, io.EOF) {
				return r, err
			}
			debug.Printf(err.Error())
			return r, nil
		}
		if r.query == nil {
			if err := r.readAhead(top); err != nil {
				return nil, err
			}
		} else {
			if err := r.jquery(top); err != nil {
				return nil, err
			}
		}
	}

	return r, nil
}

func (r *JSONReader) jquery(top interface{}) error {
	iter := r.query.Run(top)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			log.Printf("ERROR: gojq [%s] %s", r.query, err.Error())
			continue
		}
		if err := r.readAhead(v); err != nil {
			return err
		}
	}
	return nil
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

func (r *JSONReader) readAhead(top interface{}) error {
	switch m := top.(type) {
	case []interface{}:
		// []
		r.preRead = make([]map[string]string, 0, len(m))
		if r.reader.More() {
			pre, names, err := etcRow(m)
			if err != nil {
				return err
			}

			r.appendNames(names)
			r.preRead = append(r.preRead, pre)
			return nil
		}

		for _, v := range m {
			pre, names, err := topLevel(v)
			if err != nil {
				return err
			}

			r.appendNames(names)
			r.preRead = append(r.preRead, pre)
		}
		return nil
	default:
		pre, names, err := topLevel(m)
		if err != nil {
			return err
		}
		r.appendNames(names)
		r.preRead = append(r.preRead, pre)
	}
	return nil
}

// appendNames adds multiple names for the argument to be unique.
func (r *JSONReader) appendNames(names []string) {
	for _, name := range names {
		if !r.already[name] {
			r.already[name] = true
			r.names = append(r.names, name)
		}
	}
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
	if r.limitRead {
		return nil, io.EOF
	}

	var data interface{}
	err := r.reader.Decode(&data)
	if err != nil {
		return nil, err
	}

	if r.query != nil {
		// json query.
		return r.queryRun(row, data)
	}
	return r.rowParse(row, data), nil
}

func (r *JSONReader) queryRun(row []interface{}, jsonRow interface{}) ([]interface{}, error) {
	iter := r.query.Run(jsonRow)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			debug.Printf("query %s", err.Error())
			continue
		}
		row = r.rowParse(row, v)
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
