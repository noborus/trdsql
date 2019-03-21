package main

import (
	"io"

	"github.com/noborus/tbln"
)

// TBLNIn provides methods of the Input interface
type TBLNIn struct {
	reader  *tbln.Reader
	preRead [][]interface{}
	names   []string
	types   []string
}

// TBLNOut provides methods of the Output interface
type TBLNOut struct {
	writer  *tbln.Writer
	results []string
}

func (trdsql *TRDSQL) tblnInputNew(r io.Reader) (Input, error) {
	tb := &TBLNIn{}
	tb.reader = tbln.NewReader(r)
	return tb, nil
}

// GetColumn is reads the specified number of rows and determines the column name.
func (tr *TBLNIn) GetColumn(rowNum int) ([]string, error) {
	rec, err := tr.reader.ReadRow()
	if err != nil {
		return nil, err
	}
	tr.preRead = make([][]interface{}, 0)
	row := make([]interface{}, len(rec))
	for i, c := range rec {
		row[i] = c
	}
	tr.preRead = append(tr.preRead, row)
	return tr.reader.Names, nil
}

// GetTypes is reads the specified number of rows and determines the column type.
func (tr *TBLNIn) GetTypes() ([]string, error) {
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
	tb := &TBLNOut{}
	tb.writer = tbln.NewWriter(trdsql.outStream)
	return tb
}

// First is preparation
func (tb *TBLNOut) First(columns []string, types []string) error {
	d := tbln.NewDefinition()
	err := d.SetNames(columns)
	if err != nil {
		return err
	}
	err = d.SetTypes(types)
	if err != nil {
		return err
	}
	tb.writer.WriteDefinition(d)
	tb.results = make([]string, len(columns))
	return nil
}

// RowWrite is Addition to array
func (tb *TBLNOut) RowWrite(values []interface{}, columns []string) error {
	for i, col := range values {
		tb.results[i] = valString(col)
	}
	return tb.writer.WriteRow(tb.results)
}

// Last is Actual output
func (tb *TBLNOut) Last() error {
	return nil
}
