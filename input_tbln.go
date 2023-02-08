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
	inNULL    string
	preRead   [][]interface{}
	limitRead bool
	needNULL  bool
}

// NewTBLNReader returns TBLNRead and error.
func NewTBLNReader(reader io.Reader, opts *ReadOpts) (*TBLNRead, error) {
	r := &TBLNRead{}
	r.reader = tbln.NewReader(reader)
	r.limitRead = opts.InLimitRead

	r.needNULL = opts.InNeedNULL
	r.inNULL = opts.InNULL

	rec, err := r.reader.ReadRow()
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return r, err
		}
		debug.Printf(err.Error())
		return r, nil
	}

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

	r.preRead = make([][]interface{}, 0, opts.InPreRead)
	r.preRead = append(r.preRead, r.recToRow(rec))
	for n := 1; n < opts.InPreRead; n++ {
		rec, err := r.reader.ReadRow()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return r, err
			}
			debug.Printf(err.Error())
			return r, nil
		}
		r.preRead = append(r.preRead, r.recToRow(rec))
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
	row = r.recToRow(rec)
	return row, nil
}

func (r *TBLNRead) recToRow(rec []string) []interface{} {
	row := make([]interface{}, len(rec))
	for i, c := range rec {
		if c != "" {
			row[i] = c
		}
		if r.needNULL {
			if row[i] == r.inNULL {
				row[i] = nil
			}
		}
	}
	return row
}
