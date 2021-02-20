package trdsql

// Make a table from json and path.
import (
	"encoding/json"
	"errors"
	"io"

	"github.com/Jeffail/gabs/v2"
)

// JSONPATHReader provides methods of the Reader interface.
type JSONPATHReader struct {
	reader  *json.Decoder
	preRead []map[string]string
	path    string
	names   []string
	types   []string
}

// NewJSONPATHReader returns JSONPATHReader and error.
func NewJSONPATHReader(reader io.Reader, opts *ReadOpts) (*JSONPATHReader, error) {
	r := &JSONPATHReader{}
	r.reader = json.NewDecoder(reader)
	r.path = opts.InPath

	for i := 0; i < opts.InPreRead; i++ {
		jsonParsed, err := gabs.ParseJSONDecoder(r.reader)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return r, err
			}
			debug.Printf(err.Error())
			return r, nil
		}
		if opts.InPath != "" {
			jsonParsed = jsonParsed.Path(r.path)
		}
		r, err = r.readAhead(jsonParsed.Data())
		if err != nil {
			return nil, err
		}
	}

	return r, nil
}

// Names returns column names.
func (r *JSONPATHReader) Names() ([]string, error) {
	return r.names, nil
}

// Types returns column types.
// All JSONPATH types return the DefaultDBType.
func (r *JSONPATHReader) Types() ([]string, error) {
	r.types = make([]string, len(r.names))
	for i := 0; i < len(r.names); i++ {
		r.types[i] = DefaultDBType
	}
	return r.types, nil
}

func (r *JSONPATHReader) readAhead(top interface{}) (*JSONPATHReader, error) {
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
func (r *JSONPATHReader) PreReadRow() [][]interface{} {
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
func (r *JSONPATHReader) ReadRow(row []interface{}) ([]interface{}, error) {
	jsonParsed, err := gabs.ParseJSONDecoder(r.reader)
	if err != nil {
		return nil, err
	}

	if r.path != "" {
		jsonParsed = jsonParsed.Path(r.path)
	}
	jsonRow := jsonParsed.Data()

	return r.rowParse(row, jsonRow), nil
}

func (r *JSONPATHReader) rowParse(row []interface{}, jsonRow interface{}) []interface{} {
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
