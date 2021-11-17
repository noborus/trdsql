package trdsql

import (
	"errors"
	"io"
	"strconv"

	"github.com/noborus/tbln"
)

// TBLNRead provides methods of the Reader interface.
type TBLNRead struct {
	reader    tbln.Reader
	preRead   [][]interface{}
	limitRead bool
}

// NewTBLNReader returns TBLNRead and error.
func NewTBLNReader(reader io.Reader, opts *ReadOpts) (*TBLNRead, error) {
	r := &TBLNRead{}
	r.reader = tbln.NewReader(reader)

	rec, err := r.reader.ReadRow()
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return r, err
		}
		debug.Printf(err.Error())
		return r, nil
	}
	r.preRead = make([][]interface{}, 1)
	row := make([]interface{}, len(rec))
	for i, c := range rec {
		row[i] = c
	}
	r.preRead[0] = row

	r.limitRead = opts.InLimitRead

	// SetNames if there is no names header.
	d := r.reader.GetDefinition()
	names := d.Names()
	if len(names) == 0 {
		names = make([]string, len(rec))
		for i := range rec {
			names[i] = "c" + strconv.Itoa(i+1)
		}
		if err := d.SetNames(names); err != nil {
			return r, err
		}
	}

	// SetTypes if there is no types header.
	types := d.Types()
	if len(types) == 0 {
		types = make([]string, len(rec))
		for i := range rec {
			types[i] = DefaultDBType
		}

		if err := d.SetTypes(types); err != nil {
			return r, err
		}
	}

	return r, nil
}

// Names returns column names.
func (r *TBLNRead) Names() ([]string, error) {
	reader := r.reader
	if reader == nil {
		return nil, ErrNonDefinition
	}
	d := reader.GetDefinition()
	return d.Names(), nil
}

// Types returns column types.
func (r *TBLNRead) Types() ([]string, error) {
	reader := r.reader
	if reader == nil {
		return nil, ErrNonDefinition
	}
	d := reader.GetDefinition()
	return d.Types(), nil
}

// PreReadRow is returns only columns that store preread rows.
func (r *TBLNRead) PreReadRow() [][]interface{} {
	return r.preRead
}

// ReadRow is read the rest of the row.
func (r *TBLNRead) ReadRow(row []interface{}) ([]interface{}, error) {
	if r.limitRead {
		return nil, io.EOF
	}

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
