package trdsql

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/itchyny/gojq"
)

// YAMLReader provides methods of the Reader interface.
type YAMLReader struct {
	reader    *yaml.Decoder
	query     *gojq.Query
	already   map[string]bool
	inNULL    string
	preRead   []map[string]any
	names     []string
	types     []string
	limitRead bool
	needNULL  bool
	columnNum int
}

// NewYAMLReader returns YAMLReader and error.
func NewYAMLReader(reader io.Reader, opts *ReadOpts) (*YAMLReader, error) {
	r := &YAMLReader{}

	query, err := jqParse(opts.InJQuery)
	if err != nil {
		return nil, err
	}
	r.query = query

	r.reader = yaml.NewDecoder(reader)
	r.already = make(map[string]bool)

	if err := r.yamlParse(opts); err != nil {
		return nil, err
	}

	return r, nil
}

// jqParse parses a string and returns a *gojq.Query.
func jqParse(q string) (*gojq.Query, error) {
	if q == "" {
		return nil, nil
	}
	str := trimQuoteAll(q)
	query, err := gojq.Parse(str)
	if err != nil {
		return nil, fmt.Errorf("%w gojq:(%s)", err, str)
	}
	return query, nil
}

// yamlParse parses YAML and stores it in preRead.
func (r *YAMLReader) yamlParse(opts *ReadOpts) error {
	r.limitRead = opts.InLimitRead
	r.needNULL = opts.InNeedNULL
	r.inNULL = opts.InNULL

	var top any
	for range opts.InPreRead {
		if err := r.reader.Decode(&top); err != nil {
			if !errors.Is(err, io.EOF) {
				return fmt.Errorf("%w: %s", ErrInvalidYAML, err)
			}
			debug.Printf("%s", err.Error())
			return nil
		}

		if r.query != nil {
			if err := r.jquery(top); err != nil {
				return err
			}
			return nil
		}

		if err := r.readAhead(top); err != nil {
			return err
		}
	}
	return nil
}

// jquery parses the top level of the YAML and stores it in preRead.
func (r *YAMLReader) jquery(top any) error {
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

// Names returns column names.
func (r *YAMLReader) Names() ([]string, error) {
	return r.names, nil
}

// Types returns column types.
// All YAML types return the DefaultDBType.
func (r *YAMLReader) Types() ([]string, error) {
	r.types = make([]string, len(r.names))
	for i := range r.names {
		r.types[i] = DefaultDBType
	}
	return r.types, nil
}

// readAhead parses the top level of the YAML and stores it in preRead.
func (r *YAMLReader) readAhead(top any) error {
	switch m := top.(type) {
	case []any:
		for _, v := range m {
			pre, names, err := r.topLevel(v)
			if err != nil {
				return err
			}
			r.appendNames(names)
			r.preRead = append(r.preRead, pre)
		}
	case map[string]any:
		pre, names, err := r.topLevel(m)
		if err != nil {
			return err
		}
		r.appendNames(names)
		r.preRead = append(r.preRead, pre)
	case yaml.MapSlice: // YAML object (key: value). (if UseOrderedMap is enabled).
		pre, names, err := r.objectMapSlice(m)
		if err != nil {
			return err
		}
		r.appendNames(names)
		r.preRead = append(r.preRead, pre)
	default:
		pre, names, err := r.etcRow(m)
		if err != nil {
			return err
		}
		r.appendNames(names)
		r.preRead = append(r.preRead, pre)
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

func (r *YAMLReader) topLevel(top any) (map[string]any, []string, error) {
	switch obj := top.(type) {
	case map[string]any:
		return r.objectRow(obj)
	case yaml.MapSlice:
		return r.objectMapSlice(obj)
	default:
		return r.etcRow(obj)
	}
}

// PreReadRow is returns only columns that store preRead rows.
// One YAML (not YAMLl) returns all rows with preRead.
func (r *YAMLReader) PreReadRow() [][]any {
	r.columnNum = len(r.names)
	rows := make([][]any, len(r.preRead))
	for n, v := range r.preRead {
		rows[n] = make([]any, r.columnNum)
		for i := range r.columnNum {
			rows[n][i] = v[r.names[i]]
		}

	}
	return rows
}

// ReadRow is read the rest of the row.
// Only YAMLl requires ReadRow in YAML.
func (r *YAMLReader) ReadRow() ([]any, error) {
	if r.limitRead {
		return nil, io.EOF
	}
	row := make([]any, r.columnNum)
	var data any
	if err := r.reader.Decode(&data); err != nil {
		return nil, err
	}
	v := r.rowParse(row, data)
	return v, nil
}

func (r *YAMLReader) rowParse(row []any, yamlRow any) []any {
	switch m := yamlRow.(type) {
	case map[string]any:
		for i := range r.names {
			row[i] = r.colValue(m[r.names[i]])
		}
	default:
		for i := range r.names {
			row[i] = nil
		}
		row[0] = r.colValue(yamlRow)
	}
	return row
}

// objectRow returns a map of the YAML object and the column names.
func (r *YAMLReader) objectRow(obj map[string]any) (map[string]any, []string, error) {
	names := make([]string, 0, len(obj))
	row := make(map[string]any)
	for k, v := range obj {
		names = append(names, k)
		row[k] = r.colValue(v)
	}
	return row, names, nil
}

// objectMapSlice returns a yaml.MapSlice of the YAML object and the column names.
func (r *YAMLReader) objectMapSlice(obj yaml.MapSlice) (map[string]any, []string, error) {
	names := make([]string, 0, len(obj))
	row := make(map[string]any)
	for _, item := range obj {
		key := item.Key.(string)
		names = append(names, key)
		row[key] = r.colValue(item.Value)
	}
	return row, names, nil
}

// etcRow returns 1 element with column name c1.
func (r *YAMLReader) etcRow(val any) (map[string]any, []string, error) {
	var names []string
	k := "c1"
	names = append(names, k)
	row := make(map[string]any)
	row[k] = r.colValue(val)
	return row, names, nil
}

// colValue returns a string representation of val.
// It will be YAML if val is a struct or map, otherwise it will be a string representation of val.
func (r *YAMLReader) colValue(val any) any {
	var str string
	switch t := val.(type) {
	case nil:
		return nil
	case map[string]any, []yaml.MapSlice, []any:
		b, err := yaml.Marshal(val)
		if err != nil {
			log.Printf("ERROR: YAMLString:%s", err)
		}
		str = yamlToStr(b)
	case []byte:
		str = yamlToStr(t)
	case string:
		str = yamlToStr([]byte(t))
	default:
		str = ValString(t)
	}
	// Remove the last newline.
	str = strings.TrimRight(str, "\n")
	return colValue(str, r.needNULL, r.inNULL)
}

// yamlToStr converts marshalled YAML to string.
// Values that can be converted to JSON should be JSON.
func yamlToStr(buf []byte) string {
	if !bytes.Contains(buf, []byte("\n")) {
		return ValString(buf)
	}

	// Convert to JSON if it's a YAML element.
	j, err := yaml.YAMLToJSON(buf)
	if err != nil {
		return ValString(buf)
	}
	return ValString(j)
}
