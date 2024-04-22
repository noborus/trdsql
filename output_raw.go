package trdsql

import (
	"bufio"
	"strconv"
)

// RAWWriter provides methods of the Writer interface.
type RAWWriter struct {
	writer    *bufio.Writer
	delimiter string
	endLine   string
	outNULL   string
	outHeader bool
	needNULL  bool
}

// NewRAWWriter returns RAWWriter.
func NewRAWWriter(writeOpts *WriteOpts) *RAWWriter {
	delimiter, err := strconv.Unquote(`"` + writeOpts.OutDelimiter + `"`)
	if err != nil {
		debug.Printf("%s\n", err)
	}

	w := &RAWWriter{}
	w.writer = bufio.NewWriter(writeOpts.OutStream)
	w.delimiter = delimiter
	w.outHeader = writeOpts.OutHeader
	w.endLine = "\n"
	if writeOpts.OutUseCRLF {
		w.endLine = "\r\n"
	}
	w.needNULL = writeOpts.OutNeedNULL
	w.outNULL = writeOpts.OutNULL
	return w
}

// PreWrite is output of header and preparation.
func (w *RAWWriter) PreWrite(columns []string, types []string) error {
	if !w.outHeader {
		return nil
	}
	for n, col := range columns {
		if n > 0 {
			if _, err := w.writer.WriteString(w.delimiter); err != nil {
				return err
			}
		}
		if _, err := w.writer.WriteString(col); err != nil {
			return err
		}
	}
	_, err := w.writer.WriteString(w.endLine)
	return err
}

// WriteRow is row write.
func (w *RAWWriter) WriteRow(values []any, _ []string) error {
	for n, col := range values {
		if n > 0 {
			if _, err := w.writer.WriteString(w.delimiter); err != nil {
				return err
			}
		}
		str := ValString(col)
		if col == nil && w.needNULL {
			str = w.outNULL
		}
		if _, err := w.writer.WriteString(str); err != nil {
			return err
		}
	}
	return w.writer.WriteByte('\n')
}

// PostWrite is flush.
func (w *RAWWriter) PostWrite() error {
	return w.writer.Flush()
}
