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
	tw := &TWWrite{}
	tw.writer = tablewriter.NewWriter(writeOpts.OutStream)
	tw.writer.SetAutoFormatHeaders(false)
	if markdown {
		tw.writer.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		tw.writer.SetCenterSeparator("|")
	}
	return tw
}

// First is preparation
func (tw *TWWrite) First(columns []string, types []string) error {
	tw.writer.SetHeader(columns)
	tw.results = make([]string, len(columns))

	return nil
}

// WriteRow is Addition to array
func (tw *TWWrite) WriteRow(values []interface{}, columns []string) error {
	for i, col := range values {
		tw.results[i] = ValString(col)
	}
	tw.writer.Append(tw.results)
	return nil
}

// Last is Actual output
func (tw *TWWrite) Last() error {
	tw.writer.Render()
	return nil
}
