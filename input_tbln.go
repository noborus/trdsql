package trdsql

import (
	"io"

	"github.com/noborus/tbln"
)

// TBLNRead provides methods of the Reader interface
type TBLNRead struct {
	reader  tbln.Reader
	preRead [][]interface{}
}

func NewTBLNReader(r io.Reader) (Reader, error) {
	tr := &TBLNRead{}
	tr.reader = tbln.NewReader(r)
	return tr, nil
}

// GetColumn is reads the specified number of rows and determines the column name.
func (tr *TBLNRead) GetColumn(rowNum int) ([]string, error) {
	rec, err := tr.reader.ReadRow()
	if err != nil {
		return nil, err
	}
	tr.preRead = make([][]interface{}, 1)
	row := make([]interface{}, len(rec))

	for i, c := range rec {
		row[i] = c
	}
	tr.preRead[0] = row
	d := tr.reader.GetDefinition()
	return d.Names(), nil
}

// GetTypes is reads the specified number of rows and determines the column type.
func (tr *TBLNRead) GetTypes() ([]string, error) {
	d := tr.reader.GetDefinition()
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
func (tr *TBLNRead) PreReadRow() [][]interface{} {
	return tr.preRead
}

// ReadRow is read the rest of the row.
func (tr *TBLNRead) ReadRow(row []interface{}) ([]interface{}, error) {
	rec, err := tr.reader.ReadRow()
	if err != nil {
		return row, err
	}
	row = make([]interface{}, len(rec))
	for i, c := range rec {
		row[i] = c
	}
	return row, nil
}
