package trdsql

import (
	"github.com/olekukonko/tablewriter"
)

// TWWriter is tablewriter struct
type TWWriter struct {
	writer  *tablewriter.Table
	results []string
}

// NewTWWriter returns TWWriter.
func NewTWWriter(writeOpts *WriteOpts, markdown bool) *TWWriter {
	w := &TWWriter{}
	w.writer = tablewriter.NewWriter(writeOpts.OutStream)
	w.writer.SetAutoFormatHeaders(false)
	w.writer.SetAutoWrapText(!writeOpts.OutNoWrap)
	if markdown {
		w.writer.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		w.writer.SetCenterSeparator("|")
	}
	return w
}

// PreWrite is preparation.
func (w *TWWriter) PreWrite(columns []string, types []string) error {
	w.writer.SetHeader(columns)
	w.results = make([]string, len(columns))

	return nil
}

// WriteRow is Addition to array.
func (w *TWWriter) WriteRow(values []interface{}, columns []string) error {
	for i, col := range values {
		w.results[i] = ValString(col)
	}
	w.writer.Append(w.results)
	return nil
}

// PostWrite is Actual output.
func (w *TWWriter) PostWrite() error {
	w.writer.Render()
	return nil
}
