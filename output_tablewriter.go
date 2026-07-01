package trdsql

import (
	"strings"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
)

// TWWriter renders rows as an ASCII/Markdown table.
type TWWriter struct {
	writeOpts *WriteOpts
	writer    *tablewriter.Table
	outNULL   string
	needNULL  bool
	markdown  bool
}

// NewTWWriter returns a TWWriter configured with output options.
func NewTWWriter(writeOpts *WriteOpts, markdown bool) *TWWriter {
	w := &TWWriter{}
	w.writeOpts = writeOpts
	w.needNULL = writeOpts.OutNeedNULL
	w.outNULL = writeOpts.OutNULL
	w.markdown = markdown
	return w
}

func toRowAutoWrap(noWrap bool) int {
	if noWrap {
		return tw.WrapNone
	}
	return tw.WrapNormal
}

// PreWrite is preparation.
func (w *TWWriter) PreWrite(columns []string, types []string) error {
	w.writer = tablewriter.NewTable(w.writeOpts.OutStream,
		tablewriter.WithHeaderAutoFormat(tw.Off),
		tablewriter.WithHeader(columns),
		tablewriter.WithRowAutoWrap(toRowAutoWrap(w.writeOpts.OutNoWrap)),
		tablewriter.WithRendition(tw.Rendition{
			Symbols: tw.NewSymbols(tw.StyleASCII),
		}),
	)
	if w.markdown {
		w.writer.Options(tablewriter.WithRendition(tw.Rendition{
			Borders: tw.Border{Top: tw.Off, Bottom: tw.Off},
			Symbols: tw.NewSymbols(tw.StyleMarkdown),
		}))
	}
	return nil
}

// WriteRow is Addition to array.
func (w *TWWriter) WriteRow(values []any, columns []string) error {
	results := make([]string, len(values))
	for i, col := range values {
		str := ValString(col)
		if w.markdown {
			str = strings.ReplaceAll(str, `|`, `\|`)
		}
		if col == nil && w.needNULL {
			str = w.outNULL
		}
		results[i] = str
	}
	w.writer.Append(results)
	return nil
}

// PostWrite is actual output.
func (w *TWWriter) PostWrite() error {
	w.writer.Render()
	return nil
}
