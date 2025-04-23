package trdsql

import (
	"errors"
	"io"

	"github.com/noborus/guesswidth"
)

// GWReader provides methods of the Reader interface.
type GWReader struct {
	reader    *guesswidth.GuessWidth
	scanNum   int
	preRead   int
	inNULL    string
	names     []string
	types     []string
	limitRead bool
	needNULL  bool
	columnNum int
}

// NewGWReader returns GWReader and error.
func NewGWReader(reader io.Reader, opts *ReadOpts) (*GWReader, error) {
	r := &GWReader{}
	r.reader = guesswidth.NewReader(reader)
	r.reader.TrimSpace = true
	r.limitRead = opts.InLimitRead
	r.reader.Header = opts.InSkip
	r.scanNum = 1000
	r.needNULL = opts.InNeedNULL
	r.inNULL = opts.InNULL
	r.preRead = opts.InPreRead
	if r.preRead > r.scanNum {
		r.scanNum = r.preRead
	}
	r.reader.Scan(r.scanNum)
	for range opts.InSkip {
		if _, err := r.reader.Read(); err != nil {
			if errors.Is(err, io.EOF) {
				return r, nil
			}
		}
	}
	names, err := r.reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return r, nil
		}
		return nil, err
	}
	r.names = names
	r.setColumnType()
	r.columnNum = len(r.names)
	return r, nil
}

func (r *GWReader) setColumnType() {
	if r.names == nil {
		return
	}
	r.types = make([]string, len(r.names))
	for i := range r.names {
		r.types[i] = DefaultDBType
	}
}

// Names returns column names.
func (r *GWReader) Names() ([]string, error) {
	return r.names, nil
}

// Types returns column types.
// All GW types return the DefaultDBType.
func (r *GWReader) Types() ([]string, error) {
	return r.types, nil
}

// PreReadRow is returns only columns that store preread rows.
func (r *GWReader) PreReadRow() [][]any {
	rows := make([][]any, r.preRead)
	for n := range r.preRead {
		record, err := r.reader.Read()
		if err != nil {
			return rows
		}
		rows[n] = make([]any, len(r.names))
		for i := range r.names {
			rows[n][i] = colValue(record[i], r.needNULL, r.inNULL)
		}
	}
	return rows
}

// ReadRow is read the rest of the row.
func (r *GWReader) ReadRow() ([]any, error) {
	if r.limitRead {
		return nil, io.EOF
	}

	row := make([]any, r.columnNum)
	record, err := r.reader.Read()
	if err != nil {
		return row, err
	}
	for i := range row {
		row[i] = colValue(record[i], r.needNULL, r.inNULL)
	}
	return row, nil
}
