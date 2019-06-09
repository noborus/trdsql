package trdsql

import (
	"github.com/noborus/tbln"
)

// TBLNWrite provides methods of the Output interface
type TBLNWrite struct {
	writer  *tbln.Writer
	results []string
}

func NewTBLNWrite(writeOpts WriteOpts) *TBLNWrite {
	w := &TBLNWrite{}
	w.writer = tbln.NewWriter(writeOpts.OutStream)
	return w
}

// PreWrite is preparation
func (w *TBLNWrite) PreWrite(columns []string, types []string) error {
	d := tbln.NewDefinition()
	err := d.SetNames(columns)
	if err != nil {
		return err
	}
	err = d.SetTypes(ConvertTypes(types))
	if err != nil {
		return err
	}
	err = w.writer.WriteDefinition(d)
	if err != nil {
		return err
	}
	w.results = make([]string, len(columns))
	return nil
}

// WriteRow is Addition to array
func (w *TBLNWrite) WriteRow(values []interface{}, columns []string) error {
	for i, col := range values {
		w.results[i] = ValString(col)
	}
	return w.writer.WriteRow(w.results)
}

// PostWrite is Actual output
func (w *TBLNWrite) PostWrite() error {
	return nil
}
