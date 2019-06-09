package trdsql

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// RawWrite provides methods of the Output interface
type RawWrite struct {
	writer    *bufio.Writer
	results   []string
	sep       string
	outHeader bool
}

func NewRAWWrite(writeOpts WriteOpts) *RawWrite {
	var err error
	w := &RawWrite{}
	w.writer = bufio.NewWriter(writeOpts.OutStream)
	w.sep, err = strconv.Unquote(`"` + writeOpts.OutDelimiter + `"`)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	w.outHeader = writeOpts.OutHeader
	return w
}

// PreWrite is output of header and preparation
func (w *RawWrite) PreWrite(columns []string, types []string) error {
	if w.outHeader {
		_, err := fmt.Fprint(w.writer, strings.Join(columns, w.sep), "\n")
		if err != nil {
			debug.Printf("%s\n", err)
		}
	}
	w.results = make([]string, len(columns))
	return nil
}

// WriteRow is row output
func (w *RawWrite) WriteRow(values []interface{}, columns []string) error {
	for i, col := range values {
		w.results[i] = ValString(col)
	}
	_, err := fmt.Fprint(w.writer, strings.Join(w.results, w.sep), "\n")
	if err != nil {
		debug.Printf("%s\n", err)
	}
	return nil
}

// PostWrite is flush
func (w *RawWrite) PostWrite() error {
	return w.writer.Flush()
}
