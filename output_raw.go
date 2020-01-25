package trdsql

import (
	"bufio"
	"strconv"
)

// RAWWriter provides methods of the Writer interface.
type RAWWriter struct {
	writer    *bufio.Writer
	sep       string
	outHeader bool
}

// NewRAWWriter returns RAWWriter.
func NewRAWWriter(writeOpts *WriteOpts) *RAWWriter {
	var err error
	w := &RAWWriter{}
	w.writer = bufio.NewWriter(writeOpts.OutStream)
	w.sep, err = strconv.Unquote(`"` + writeOpts.OutDelimiter + `"`)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	w.outHeader = writeOpts.OutHeader
	return w
}

// PreWrite is output of header and preparation.
func (w *RAWWriter) PreWrite(columns []string, types []string) error {
	if !w.outHeader {
		return nil
	}
	for n, col := range columns {
		if n > 0 {
			if _, err := w.writer.WriteString(w.sep); err != nil {
				return err
			}
		}
		_, err := w.writer.WriteString(col)
		if err != nil {
			return err
		}
	}
	return w.writer.WriteByte('\n')
}

// WriteRow is row write.
func (w *RAWWriter) WriteRow(values []interface{}, columns []string) error {
	for n, col := range values {
		if n > 0 {
			if _, err := w.writer.WriteString(w.sep); err != nil {
				return err
			}
		}
		_, err := w.writer.WriteString(ValString(col))
		if err != nil {
			return err
		}
	}
	return w.writer.WriteByte('\n')
}

// PostWrite is flush.
func (w *RAWWriter) PostWrite() error {
	return w.writer.Flush()
}
