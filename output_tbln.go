package trdsql

import (
	"github.com/noborus/tbln"
)

// TBLNOut provides methods of the Output interface
type TBLNOut struct {
	writer  *tbln.Writer
	results []string
}

func (trdsql *TRDSQL) tblnOutNew() *TBLNOut {
	tw := &TBLNOut{}
	tw.writer = tbln.NewWriter(trdsql.OutStream)
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

// WriteRow is Addition to array
func (tw *TBLNOut) WriteRow(values []interface{}, columns []string) error {
	for i, col := range values {
		tw.results[i] = valString(col)
	}
	return tw.writer.WriteRow(tw.results)
}

// Last is Actual output
func (tw *TBLNOut) Last() error {
	return nil
}
