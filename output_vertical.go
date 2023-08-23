package trdsql

import (
	"bufio"
	"fmt"
	"strings"

	runewidth "github.com/mattn/go-runewidth"
	"golang.org/x/term"
)

// VFWriter is Vertical Format output.
type VFWriter struct {
	writer    *bufio.Writer
	outNULL   string
	header    []string
	termWidth int
	hSize     int
	count     int
	needNULL  bool
}

// NewVFWriter returns VFWriter.
func NewVFWriter(writeOpts *WriteOpts) *VFWriter {
	var err error
	w := &VFWriter{}
	w.writer = bufio.NewWriter(writeOpts.OutStream)
	w.termWidth, _, err = term.GetSize(0)
	if err != nil {
		w.termWidth = 40
	}
	w.needNULL = writeOpts.OutNeedNULL
	w.outNULL = writeOpts.OutNULL
	return w
}

// PreWrite is preparation.
func (w *VFWriter) PreWrite(columns []string, types []string) error {
	w.header = make([]string, len(columns))
	w.hSize = 0
	for i, col := range columns {
		if w.hSize < runewidth.StringWidth(col) {
			w.hSize = runewidth.StringWidth(col)
		}
		w.header[i] = col
	}
	return nil
}

// WriteRow is Actual output.
func (w *VFWriter) WriteRow(values []interface{}, columns []string) error {
	w.count++
	_, err := fmt.Fprintf(w.writer,
		"---[ %d]%s\n", w.count, strings.Repeat("-", (w.termWidth-16)))
	if err != nil {
		debug.Printf("%s\n", err)
	}
	for i, col := range w.header {
		v := w.hSize - runewidth.StringWidth(col)
		str := ""
		if values[i] == nil && w.needNULL {
			str = w.outNULL
		} else {
			str = strings.TrimRight(ValString(values[i]), "\n")
		}

		_, err := fmt.Fprintf(w.writer,
			"%s%s | %-s\n",
			strings.Repeat(" ", v+2),
			col,
			str)
		if err != nil {
			debug.Printf("%s\n", err)
		}
	}
	return nil
}

// PostWrite is flush.
func (w *VFWriter) PostWrite() error {
	return w.writer.Flush()
}
