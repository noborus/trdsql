package trdsql

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// RawWriter provides methods of the Writer interface.
type RawWriter struct {
	writer    *bufio.Writer
	results   []string
	sep       string
	outHeader bool
}

// NewRAWWriter returns RawWriter.
func NewRAWWriter(writeOpts WriteOpts) *RawWriter {
	var err error
	w := &RawWriter{}
	w.writer = bufio.NewWriter(writeOpts.OutStream)
	w.sep, err = strconv.Unquote(`"` + writeOpts.OutDelimiter + `"`)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	w.outHeader = writeOpts.OutHeader
	return w
}

// PreWrite is output of header and preparation.
func (w *RawWriter) PreWrite(columns []string, types []string) error {
	if w.outHeader {
		_, err := fmt.Fprint(w.writer, strings.Join(columns, w.sep), "\n")
		if err != nil {
			debug.Printf("%s\n", err)
		}
	}
	w.results = make([]string, len(columns))
	return nil
}

// WriteRow is row write.
func (w *RawWriter) WriteRow(values []interface{}, columns []string) error {
	for i, col := range values {
		w.results[i] = ValString(col)
	}
	_, err := fmt.Fprint(w.writer, strings.Join(w.results, w.sep), "\n")
	if err != nil {
		debug.Printf("%s\n", err)
	}
	return nil
}

// PostWrite is flush.
func (w *RawWriter) PostWrite() error {
	return w.writer.Flush()
}
