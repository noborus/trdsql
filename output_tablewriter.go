package trdsql

import (
	"strings"

	"github.com/noborus/termhyo"
)

// TWWriter provides methods of the Writer interface.
type TWWriter struct {
	writeOpts *WriteOpts
	writer    *termhyo.Table
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
	cols := make([]termhyo.Column, len(columns))

	for i, name := range columns {
		cols[i] = termhyo.Column{
			Title: name,
			Width: 0,
			Align: "left",
		}
	}
	if w.markdown {
		w.writer = termhyo.NewTable(w.writeOpts.OutStream, cols, termhyo.Border(termhyo.MarkdownStyle))
		if w.writeOpts.OutNoAlign {
			w.writer.SetAutoAlign(false)
		}
	} else {
		w.writer = termhyo.NewTable(w.writeOpts.OutStream, cols, termhyo.Border(termhyo.ASCIIStyle))
		if w.writeOpts.OutNoAlign {
			w.writer.SetHeaderStyleWithoutBorders(termhyo.DefaultHeaderStyle())
			w.writer.SetAutoAlign(false)
		}
	}
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
	w.writer.AddRow(w.results...)
	return nil
}

// PostWrite is actual output.
func (w *TWWriter) PostWrite() error {
	w.writer.Render()
	return nil
}
