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
	query     *gojq.Query
	already   map[string]bool
	inNULL    string
	preRead   []map[string]any
	names     []string
	types     []string
	limitRead bool
	needNULL  bool
}

// NewJSONReader returns JSONReader and error.
func NewJSONReader(reader io.Reader, opts *ReadOpts) (*JSONReader, error) {
	r := &JSONReader{}
	r.reader = json.NewDecoder(reader)
	r.reader.UseNumber()
	r.already = make(map[string]bool)
	var top any

	if opts.InJQuery != "" {
		str := trimQuoteAll(opts.InJQuery)
		query, err := gojq.Parse(str)
		if err != nil {
			return nil, fmt.Errorf("%w gojq:(%s)", err, opts.InJQuery)
		}
		r.query = query
	}

	r.limitRead = opts.InLimitRead
	r.needNULL = opts.InNeedNULL
	r.inNULL = opts.InNULL

	for i := 0; i < opts.InPreRead; i++ {
		if err := r.reader.Decode(&top); err != nil {
			if !errors.Is(err, io.EOF) {
				return r, fmt.Errorf("%w: %s", ErrInvalidJSON, err)
			}
			debug.Printf(err.Error())
			return r, nil
		}

		if r.query != nil {
			if err := r.jqueryRun(top); err != nil {
				return nil, err
			}
			return r, nil
		}

		if err := r.readAhead(top); err != nil {
			return nil, err
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

// readAhead parses the top level of the JSON and stores it in preRead.
func (r *JSONReader) readAhead(top any) error {
	switch m := top.(type) {
	case []any:
		// []
		r.preRead = make([]map[string]any, 0, len(m))
		if r.reader.More() {
			pre, names, err := r.etcRow(m)
			if err != nil {
				return err
			}
			r.appendNames(names)
			r.preRead = append(r.preRead, pre)
			return nil
		}

		for _, v := range m {
			pre, names, err := r.topLevel(v)
			if err != nil {
				return err
			}
			r.appendNames(names)
			r.preRead = append(r.preRead, pre)
		}
		return nil
	default:
		pre, names, err := r.topLevel(m)
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

func (r *JSONReader) topLevel(top any) (map[string]any, []string, error) {
	switch obj := top.(type) {
	case map[string]any:
		return r.objectRow(obj)
	default:
		return r.etcRow(obj)
	}
}

// PreReadRow is returns only columns that store preRead rows.
// One json (not jsonl) returns all rows with preRead.
func (r *JSONReader) PreReadRow() [][]any {
	rows := make([][]any, len(r.preRead))
	for n, v := range r.preRead {
		rows[n] = make([]any, len(r.names))
		for i := range r.names {
			rows[n][i] = v[r.names[i]]
			if r.needNULL {
				rows[n][i] = replaceNULL(r.inNULL, rows[n][i])
			}
		}
	}
	return rows
}

// ReadRow is read the rest of the row.
// Only jsonl requires ReadRow in json.
func (r *JSONReader) ReadRow(row []any) ([]any, error) {
	if r.limitRead {
		return nil, io.EOF
	}

	var data any
	if err := r.reader.Decode(&data); err != nil {
		return nil, err
	}

	if r.query != nil {
		return r.jqueryRunJsonl(row, data)
	}
	return r.rowParse(row, data), nil
}

func (r *JSONReader) rowParse(row []any, jsonRow any) []any {
	switch m := jsonRow.(type) {
	case map[string]any:
		for i := range r.names {
			row[i] = r.jsonString(m[r.names[i]])
		}
	default:
		for i := range r.names {
			row[i] = nil
		}
		row[0] = r.jsonString(jsonRow)
	}
	return row
}

func (r *JSONReader) objectRow(obj map[string]any) (map[string]any, []string, error) {
	// {"a":"b"} object
	names := make([]string, 0, len(obj))
	row := make(map[string]any)
	for k, v := range obj {
		names = append(names, k)
		row[k] = r.jsonString(v)
	}
	return row, names, nil
}

func (r *JSONReader) etcRow(val any) (map[string]any, []string, error) {
	// ex. array array
	// [["a"],
	//  ["b"]]
	var names []string
	k := "c1"
	names = append(names, k)
	row := make(map[string]any)
	row[k] = r.jsonString(val)
	return row, names, nil
}

// jqueryRun is a gojq.Run for json.
func (r *JSONReader) jqueryRun(top any) error {
	iter := r.query.Run(top)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			return fmt.Errorf("%w gojq:(%s) ", err, r.query)
		}
		if err := r.readAhead(v); err != nil {
			return err
		}
	}
	return nil
}

// jqueryRunJsonl gojq.Run for rows of jsonl.
func (r *JSONReader) jqueryRunJsonl(row []any, jsonRow any) ([]any, error) {
	iter := r.query.Run(jsonRow)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			debug.Printf("%s gojq: %s", err.Error(), r.query)
			continue
		}
		row = r.rowParse(row, v)
	}
	return row, nil
}

// jsonString returns the string of the argument.
func (r *JSONReader) jsonString(val any) any {
	var str string
	switch val.(type) {
	case nil:
		return nil
	case map[string]any, []any:
		b, err := json.Marshal(val)
		if err != nil {
			log.Printf("ERROR: jsonString:%s", err)
		}
		str = ValString(b)
	default:
		str = ValString(val)
	}
	if r.needNULL {
		return replaceNULL(r.inNULL, str)
	}
	return str
}
