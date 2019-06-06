package trdsql

import (
	"github.com/noborus/tbln"
)

// TBLNWrite provides methods of the Output interface
type TBLNWrite struct {
	writer  *tbln.Writer
	results []string
}

func NewTBLNWrite() *TBLNWrite {
	tw := &TBLNWrite{}
	tw.writer = tbln.NewWriter(DefaultWriteOpts.OutStream)
	return tw
}

// First is preparation
func (tw *TBLNWrite) First(columns []string, types []string) error {
	d := tbln.NewDefinition()
	err := d.SetNames(columns)
	if err != nil {
		return err
	}
	err = d.SetTypes(ConvertTypes(types))
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
func (tw *TBLNWrite) WriteRow(values []interface{}, columns []string) error {
	for i, col := range values {
		tw.results[i] = ValString(col)
	}
	return tw.writer.WriteRow(tw.results)
}

// Last is Actual output
func (tw *TBLNWrite) Last() error {
	return nil
}
