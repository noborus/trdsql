package trdsql

import (
	"strings"

	"github.com/olekukonko/tablewriter"
)

// TWWriter provides methods of the Writer interface.
type TWWriter struct {
	writeOpts *WriteOpts
	writer    *tablewriter.Table
	outNULL   string
	results   []string
	needNULL  bool
	markdown  bool
}

// NewTWWriter returns TWWriter.
func NewTWWriter(writeOpts *WriteOpts, markdown bool) *TWWriter {
	w := &TWWriter{}
	w.writeOpts = writeOpts
	w.needNULL = writeOpts.OutNeedNULL
	w.outNULL = writeOpts.OutNULL
	w.markdown = markdown
	return w
}

// PreWrite is preparation.
func (w *TWWriter) PreWrite(columns []string, types []string) error {
	w.writer = tablewriter.NewWriter(w.writeOpts.OutStream)
	w.writer.SetAutoFormatHeaders(false)
	w.writer.SetAutoWrapText(!w.writeOpts.OutNoWrap)
	if w.markdown {
		w.writer.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		w.writer.SetCenterSeparator("|")
	}
	w.writer.SetHeader(columns)
	w.results = make([]string, len(columns))

	return nil
}

// WriteRow is Addition to array.
func (w *TWWriter) WriteRow(values []any, columns []string) error {
	for i, col := range values {
		str := ValString(col)
		if w.markdown {
			str = strings.ReplaceAll(str, `|`, `\|`)
		}
		if col == nil && w.needNULL {
			str = w.outNULL
		}
		w.results[i] = str
	}
	w.writer.Append(w.results)
	return nil
}

// PostWrite is actual output.
func (w *TWWriter) PostWrite() error {
	w.writer.Render()
	return nil
}
