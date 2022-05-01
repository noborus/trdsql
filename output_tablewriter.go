package trdsql

import (
	"github.com/olekukonko/tablewriter"
)

// TWWriter provides methods of the Writer interface.
type TWWriter struct {
	writer   *tablewriter.Table
	results  []string
	needNULL bool
	outNULL  string
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
	w.needNULL = writeOpts.OutNeedNULL
	w.outNULL = writeOpts.OutNULL
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
		str := ""
		if col == nil && w.needNULL {
			str = w.outNULL
		} else {
			str = ValString(col)
		}
		w.results[i] = str
	}
	w.writer.Append(w.results)
	return nil
}

// PostWrite is Actual output.
func (w *TWWriter) PostWrite() error {
	w.writer.Render()
	return nil
}
