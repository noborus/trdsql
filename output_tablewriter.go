package trdsql

import (
	"strings"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
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
func (w *TWWriter) PreWrite(columns, types []string) error {
	var opts []tablewriter.Option
	opts = append(opts, tablewriter.WithHeaderAutoFormat(tw.Off))

	if !w.writeOpts.OutNoWrap {
		opts = append(opts, tablewriter.WithRowAutoWrap(tw.WrapNormal))
	} else {
		opts = append(opts, tablewriter.WithRowAutoWrap(tw.WrapNone))
	}

	aligns := make([]tw.Align, len(types))
	for i, t := range types {
		t = strings.ToLower(t)
		if strings.Contains(t, "int") || strings.Contains(t, "float") || strings.Contains(t, "numeric") || strings.Contains(t, "decimal") || strings.Contains(t, "double") || strings.Contains(t, "real") {
			aligns[i] = tw.AlignRight
		} else {
			aligns[i] = tw.AlignLeft
		}
	}
	opts = append(opts, tablewriter.WithRowAlignmentConfig(tw.CellAlignment{PerColumn: aligns}))

	if w.markdown {
		opts = append(opts, tablewriter.WithRendition(tw.Rendition{
			Borders: tw.Border{Left: tw.On, Top: tw.Off, Right: tw.On, Bottom: tw.Off},
			Symbols: tw.NewSymbols(tw.StyleMarkdown),
		}))
	} else {
		opts = append(opts, tablewriter.WithRendition(tw.Rendition{
			Symbols: tw.NewSymbols(tw.StyleASCII),
		}))
	}

	w.writer = tablewriter.NewTable(w.writeOpts.OutStream, opts...)

	headerArgs := make([]any, len(columns))
	for i, col := range columns {
		headerArgs[i] = col
	}
	w.writer.Header(headerArgs...)

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

	rowArgs := make([]any, len(w.results))
	for i, res := range w.results {
		rowArgs[i] = res
	}
	_ = w.writer.Append(rowArgs...)
	return nil
}

// PostWrite is actual output.
func (w *TWWriter) PostWrite() error {
	_ = w.writer.Render()
	return nil
}
