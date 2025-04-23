package trdsql

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

// LTSVReader provides methods of the Reader interface.
type LTSVReader struct {
	reader    *bufio.Reader
	delimiter string
	inNULL    string
	preRead   []map[string]string
	names     []string
	types     []string
	limitRead bool
	needNULL  bool
	columnNum int
}

// NewLTSVReader returns LTSVReader and error.
func NewLTSVReader(reader io.Reader, opts *ReadOpts) (*LTSVReader, error) {
	r := &LTSVReader{}
	r.reader = bufio.NewReader(reader)
	r.delimiter = "\t"

	if opts.InSkip > 0 {
		skipRead(r, opts.InSkip)
	}

	r.limitRead = opts.InLimitRead

	r.needNULL = opts.InNeedNULL
	r.inNULL = opts.InNULL

	names := map[string]bool{}
	for range opts.InPreRead {
		row, keys, err := r.read()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return r, err
			}
			r.setColumnType()
			debug.Printf(err.Error())
			return r, nil
		}

		// Add only unique column names.
		for k := range keys {
			if !names[keys[k]] {
				names[keys[k]] = true
				r.names = append(r.names, keys[k])
			}
		}
		r.preRead = append(r.preRead, row)
	}
	r.setColumnType()
	r.columnNum = len(r.names)
	return r, nil
}

func (r *LTSVReader) setColumnType() {
	if r.names == nil {
		return
	}
	r.types = make([]string, len(r.names))
	for i := range r.names {
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
func (r *LTSVReader) PreReadRow() [][]any {
	rowNum := len(r.preRead)
	rows := make([][]any, rowNum)
	for n := range rowNum {
		rows[n] = make([]any, len(r.names))
		for i := range r.names {
			f := r.preRead[n][r.names[i]]
			rows[n][i] = colValue(f, r.needNULL, r.inNULL)
		}
	}
	return rows
}

// ReadRow is read the rest of the row.
func (r *LTSVReader) ReadRow() ([]any, error) {
	if r.limitRead {
		return nil, io.EOF
	}

	row := make([]any, r.columnNum)
	record, _, err := r.read()
	if err != nil {
		return row, err
	}
	for i, name := range r.names {
		row[i] = colValue(record[name], r.needNULL, r.inNULL)
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
			return nil, nil, ErrInvalidColumn
		}
		lvs[kv[0]] = kv[1]
		keys = append(keys, kv[0])
	}
	return lvs, keys, nil
}

func (r *LTSVReader) readline() (string, error) {
	var builder strings.Builder
	for {
		line, isPrefix, err := r.reader.ReadLine()
		if err != nil {
			return "", err
		}
		builder.Write(line)
		if isPrefix {
			continue
		}
		str := strings.TrimSpace(builder.String())
		if len(str) != 0 {
			return str, nil
		}
		builder.Reset()
	}
}
