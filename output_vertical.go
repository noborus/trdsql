package trdsql

import (
	"bufio"
	"fmt"
	"strings"

	runewidth "github.com/mattn/go-runewidth"
	"golang.org/x/crypto/ssh/terminal"
)

// VFWrite is Vertical Format output
type VFWrite struct {
	writer    *bufio.Writer
	termWidth int
	hSize     int
	header    []string
	count     int
}

func NewVFWrite(writeOpts WriteOpts) *VFWrite {
	var err error
	w := &VFWrite{}
	w.writer = bufio.NewWriter(writeOpts.OutStream)
	w.termWidth, _, err = terminal.GetSize(0)
	if err != nil {
		w.termWidth = 40
	}
	return w
}

// PreWrite is preparation
func (w *VFWrite) PreWrite(columns []string, types []string) error {
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

// WriteRow is Actual output
func (w *VFWrite) WriteRow(values []interface{}, columns []string) error {
	w.count++
	_, err := fmt.Fprintf(w.writer,
		"---[ %d]%s\n", w.count, strings.Repeat("-", (w.termWidth-16)))
	if err != nil {
		debug.Printf("%s\n", err)
	}
	for i, col := range w.header {
		v := w.hSize - runewidth.StringWidth(col)
		_, err := fmt.Fprintf(w.writer,
			"%s%s | %-s\n",
			strings.Repeat(" ", v+2),
			col,
			ValString(values[i]))
		if err != nil {
			debug.Printf("%s\n", err)
		}
	}
	return nil
}

// PostWrite is flush
func (w *VFWrite) PostWrite() error {
	return w.writer.Flush()
}
