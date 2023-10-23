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
	for i := 0; i < opts.InSkip; i++ {
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
	return r, nil
}

func (r *GWReader) setColumnType() {
	if r.names == nil {
		return
	}
	r.types = make([]string, len(r.names))
	for i := 0; i < len(r.names); i++ {
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
func (r *GWReader) PreReadRow() [][]interface{} {
	rows := make([][]interface{}, r.preRead)
	for n := 0; n < r.preRead; n++ {
		record, err := r.reader.Read()
		if err != nil {
			return rows
		}
		rows[n] = make([]interface{}, len(r.names))
		for i := 0; i < len(r.names); i++ {
			rows[n][i] = record[i]
			if r.needNULL {
				rows[n][i] = replaceNULL(r.inNULL, rows[n][i])
			}
		}
	}
	return rows
}

// ReadRow is read the rest of the row.
func (r *GWReader) ReadRow(row []interface{}) ([]interface{}, error) {
	if r.limitRead {
		return nil, io.EOF
	}

	record, err := r.reader.Read()
	if err != nil {
		return row, err
	}
	for i := 0; i < len(row); i++ {
		row[i] = record[i]
		if r.needNULL {
			row[i] = replaceNULL(r.inNULL, row[i])
		}
	}
	return row, nil
}
