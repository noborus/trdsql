package trdsql

import (
	"errors"
	"io"

	"github.com/noborus/tbln"
)

// TBLNRead provides methods of the Reader interface.
type TBLNRead struct {
	reader  tbln.Reader
	preRead [][]interface{}
}

// NewTBLNReader returns TBLNRead and error.
func NewTBLNReader(reader io.Reader) (*TBLNRead, error) {
	if reader == nil {
		return nil, errors.New("nil reader")
	}
	r := &TBLNRead{}
	r.reader = tbln.NewReader(reader)

	rec, err := r.reader.ReadRow()
	if err != nil {
		return nil, err
	}
	r.preRead = make([][]interface{}, 1)
	row := make([]interface{}, len(rec))

	for i, c := range rec {
		row[i] = c
	}
	r.preRead[0] = row

	return r, nil
}

// Names returns column names.
func (r *TBLNRead) Names() ([]string, error) {
	d := r.reader.GetDefinition()
	return d.Names(), nil
}

// Types returns column types.
func (r *TBLNRead) Types() ([]string, error) {
	d := r.reader.GetDefinition()
	names := d.Names()
	types := d.Types()
	if len(types) == 0 {
		types = make([]string, len(names))
		for i := 0; i < len(names); i++ {
			types[i] = DefaultDBType
		}
	}
	return types, nil
}

// PreReadRow is returns only columns that store preread rows.
func (r *TBLNRead) PreReadRow() [][]interface{} {
	return r.preRead
}

// ReadRow is read the rest of the row.
func (r *TBLNRead) ReadRow(row []interface{}) ([]interface{}, error) {
	rec, err := r.reader.ReadRow()
	if err != nil {
		return row, err
	}
	row = make([]interface{}, len(rec))
	for i, c := range rec {
		row[i] = c
	}
	return row, nil
}
