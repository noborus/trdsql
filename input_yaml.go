package trdsql

import (
	"errors"
	"io"
	"log"

	"github.com/goccy/go-yaml"
)

// YAMLReader provides methods of the Reader interface.
type YAMLReader struct {
	reader    *yaml.Decoder
	already   map[string]bool
	inNULL    string
	preRead   []map[string]interface{}
	names     []string
	types     []string
	limitRead bool
	needNULL  bool
}

// NewYAMLReader returns YAMLReader and error.
func NewYAMLReader(reader io.Reader, opts *ReadOpts) (*YAMLReader, error) {
	r := &YAMLReader{}
	r.reader = yaml.NewDecoder(reader)
	r.already = make(map[string]bool)
	var top interface{}

	r.limitRead = opts.InLimitRead
	r.needNULL = opts.InNeedNULL
	r.inNULL = opts.InNULL

	for i := 0; i < opts.InPreRead; i++ {
		if err := r.reader.Decode(&top); err != nil {
			if !errors.Is(err, io.EOF) {
				return r, err
			}
			debug.Printf(err.Error())
			return r, nil
		}
		if err := r.readAhead(top); err != nil {
			return nil, err
		}
	}

	return r, nil
}

// Names returns column names.
func (r *YAMLReader) Names() ([]string, error) {
	return r.names, nil
}

// Types returns column types.
// All YAML types return the DefaultDBType.
func (r *YAMLReader) Types() ([]string, error) {
	r.types = make([]string, len(r.names))
	for i := 0; i < len(r.names); i++ {
		r.types[i] = DefaultDBType
	}
	return r.types, nil
}

func (r *YAMLReader) readAhead(top interface{}) error {
	switch m := top.(type) {
	case []interface{}:
		for _, v := range m {
			pre, names, err := r.topLevel(v)
			if err != nil {
				return err
			}
			r.appendNames(names)
			r.preRead = append(r.preRead, pre)
		}
	case map[string]interface{}:
		pre, names, err := r.topLevel(m)
		if err != nil {
			return err
		}
		r.appendNames(names)
		r.preRead = append(r.preRead, pre)
	default:
		return ErrInvalidYAML
	}
	return nil
}

// appendNames adds multiple names for the argument to be unique.
func (r *YAMLReader) appendNames(names []string) {
	for _, name := range names {
		if !r.already[name] {
			r.already[name] = true
			r.names = append(r.names, name)
		}
	}
}

func (r *YAMLReader) topLevel(top interface{}) (map[string]interface{}, []string, error) {
	switch obj := top.(type) {
	case map[string]interface{}:
		return r.objectRow(obj)
	default:
		return r.etcRow(obj)
	}
}

// PreReadRow is returns only columns that store preRead rows.
// One YAML (not YAMLl) returns all rows with preRead.
func (r *YAMLReader) PreReadRow() [][]interface{} {
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
// Only YAMLl requires ReadRow in YAML.
func (r *YAMLReader) ReadRow(row []interface{}) ([]interface{}, error) {
	if r.limitRead {
		return nil, io.EOF
	}

	var data interface{}
	if err := r.reader.Decode(&data); err != nil {
		return nil, err
	}
	v := r.rowParse(row, data)
	return v, nil
}

func (r *YAMLReader) rowParse(row []interface{}, YAMLRow interface{}) []interface{} {
	switch m := YAMLRow.(type) {
	case map[string]interface{}:
		for i := range r.names {
			row[i] = r.YAMLString(m[r.names[i]])
		}
	default:
		for i := range r.names {
			row[i] = nil
		}
		row[0] = r.YAMLString(YAMLRow)
	}
	return row
}

func (r *YAMLReader) objectRow(obj map[string]interface{}) (map[string]interface{}, []string, error) {
	names := make([]string, 0, len(obj))
	row := make(map[string]interface{})
	for k, v := range obj {
		names = append(names, k)
		if v == nil {
			row[k] = nil
		} else {
			row[k] = r.YAMLString(v)
		}
	}
	return row, names, nil
}

func (r *YAMLReader) etcRow(val interface{}) (map[string]interface{}, []string, error) {
	var names []string
	k := "c1"
	names = append(names, k)
	row := make(map[string]interface{})
	row[k] = r.YAMLString(val)
	return row, names, nil
}

func (r *YAMLReader) YAMLString(val interface{}) interface{} {
	var str string
	switch val.(type) {
	case nil:
		return nil
	case map[string]interface{}, []interface{}:
		b, err := yaml.Marshal(val)
		if err != nil {
			log.Printf("ERROR: YAMLString:%s", err)
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
