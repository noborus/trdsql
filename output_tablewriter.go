package trdsql

import (
	"github.com/olekukonko/tablewriter"
)

// TWWrite is tablewriter struct
type TWWrite struct {
	writer  *tablewriter.Table
	results []string
}

func NewTWWrite(writeOpts WriteOpts, markdown bool) *TWWrite {
	w := &TWWrite{}
	w.writer = tablewriter.NewWriter(writeOpts.OutStream)
	w.writer.SetAutoFormatHeaders(false)
	if markdown {
		w.writer.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		w.writer.SetCenterSeparator("|")
	}
	return w
}

// PreWrite is preparation
func (w *TWWrite) PreWrite(columns []string, types []string) error {
	w.writer.SetHeader(columns)
	w.results = make([]string, len(columns))

	return nil
}

// WriteRow is Addition to array
func (w *TWWrite) WriteRow(values []interface{}, columns []string) error {
	for i, col := range values {
		w.results[i] = ValString(col)
	}
	w.writer.Append(w.results)
	return nil
}

// PostWrite is Actual output
func (w *TWWrite) PostWrite() error {
	w.writer.Render()
	return nil
}
