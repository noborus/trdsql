package trdsql

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

// LTSVReader provides methods of the Reader interface
type LTSVReader struct {
	reader    *bufio.Reader
	preRead   []map[string]string
	delimiter string
	names     []string
	types     []string
}

// NewLTSVReader returns LTSVReader and error.
func NewLTSVReader(reader io.Reader, opts *ReadOpts) (*LTSVReader, error) {
	r := &LTSVReader{}
	r.reader = bufio.NewReader(reader)
	r.delimiter = "\t"

	if opts.InSkip > 0 {
		skipRead(r, opts.InSkip)
	}

	names := map[string]bool{}
	for i := 0; i < opts.InPreRead; i++ {
		row, keys, err := r.read()
		if err != nil {
			if err != io.EOF {
				return r, err
			}
			r.setColumnType()
			return r, nil
		}

		// Add only unique column names.
		for k := 0; k < len(keys); k++ {
			if !names[keys[k]] {
				names[keys[k]] = true
				r.names = append(r.names, keys[k])
			}
		}
		r.preRead = append(r.preRead, row)
	}
	r.setColumnType()
	return r, nil
}

func (r *LTSVReader) setColumnType() {
	if r.names == nil {
		return
	}
	r.types = make([]string, len(r.names))
	for i := 0; i < len(r.names); i++ {
		r.types[i] = DefaultDBType
	}
}

// Names returns column names.
func (r *LTSVReader) Names() ([]string, error) {
	return r.names, nil
}

// Types returns column types.
// All LTSV types return the DefaultDBType.
func (r *LTSVReader) Types() ([]string, error) {
	return r.types, nil
}

// PreReadRow is returns only columns that store preread rows.
func (r *LTSVReader) PreReadRow() [][]interface{} {
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
func (r *LTSVReader) ReadRow(row []interface{}) ([]interface{}, error) {
	record, _, err := r.read()
	if err != nil {
		return row, err
	}
	for i, name := range r.names {
		row[i] = record[name]
	}
	return row, nil
}

func (r *LTSVReader) read() (map[string]string, []string, error) {
	line, err := r.readline()
	if err != nil {
		return nil, nil, err
	}
	columns := strings.Split(line, r.delimiter)
	lvs := make(map[string]string)
	keys := make([]string, 0, len(columns))
	for _, column := range columns {
		kv := strings.SplitN(column, ":", 2)
		if len(kv) != 2 {
			return nil, nil, errors.New("invalid column")
		}
		lvs[kv[0]] = kv[1]
		keys = append(keys, kv[0])
	}
	return lvs, keys, nil
}

func (r *LTSVReader) readline() (string, error) {
	var buffer []byte
	for {
		for {
			line, isPrefix, err := r.reader.ReadLine()
			if err != nil {
				return "", err
			}
			buffer = append(buffer, line...)
			if !isPrefix {
				break
			}
		}
		str := strings.TrimSpace(string(buffer))
		if len(str) != 0 {
			return str, nil
		}
	}
}
