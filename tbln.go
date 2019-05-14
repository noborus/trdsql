package main

import (
	"io"

	"github.com/noborus/tbln"
)

// TBLNIn provides methods of the Input interface
type TBLNIn struct {
	reader  *tbln.Reader
	preRead [][]interface{}
}

// TBLNOut provides methods of the Output interface
type TBLNOut struct {
	writer  *tbln.Writer
	results []string
}

func (trdsql *TRDSQL) tblnInputNew(r io.Reader) (Input, error) {
	tr := &TBLNIn{}
	tr.reader = tbln.NewReader(r)
	return tr, nil
}

// GetColumn is reads the specified number of rows and determines the column name.
func (tr *TBLNIn) GetColumn(rowNum int) ([]string, error) {
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
	return tr.reader.Names, nil
}

// GetTypes is reads the specified number of rows and determines the column type.
func (tr *TBLNIn) GetTypes() ([]string, error) {
	if len(tr.reader.Types) == 0 {
		tr.reader.Types = make([]string, len(tr.reader.Names))
		for i := 0; i < len(tr.reader.Names); i++ {
			tr.reader.Types[i] = "text"
		}
	}
	return tr.reader.Types, nil
}

// PreReadRow is returns only columns that store preread rows.
func (tr *TBLNIn) PreReadRow() [][]interface{} {
	return tr.preRead
}

// ReadRow is read the rest of the row.
func (tr *TBLNIn) ReadRow([]interface{}) ([]interface{}, error) {
	rec, err := tr.reader.ReadRow()
	row := make([]interface{}, len(rec))
	for i, c := range rec {
		row[i] = c
	}
	return row, err
}

func (trdsql *TRDSQL) tblnOutNew() Output {
	tw := &TBLNOut{}
	tw.writer = tbln.NewWriter(trdsql.outStream)
	return tw
}

// First is preparation
func (tw *TBLNOut) First(columns []string, types []string) error {
	d := tbln.NewDefinition()
	err := d.SetNames(columns)
	if err != nil {
		return err
	}
	err = d.SetTypes(types)
	if err != nil {
		return err
	}
	err = tw.writer.WriteDefinition(d)
	if err != nil {
		return err
	}
	tw.results = make([]string, len(columns))
	return nil
}

// RowWrite is Addition to array
func (tw *TBLNOut) RowWrite(values []interface{}, columns []string) error {
	for i, col := range values {
		tw.results[i] = valString(col)
	}
	return tw.writer.WriteRow(tw.results)
}

// Last is Actual output
func (tw *TBLNOut) Last() error {
	return nil
}
