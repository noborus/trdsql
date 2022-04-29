package trdsql

import (
	"bufio"
	"strconv"
)

// RAWWriter provides methods of the Writer interface.
type RAWWriter struct {
	writer    *bufio.Writer
	delimiter string
	outHeader bool
	endLine   string
	needNULL  bool
	outNULL   string
}

// NewRAWWriter returns RAWWriter.
func NewRAWWriter(writeOpts *WriteOpts) *RAWWriter {
	var err error
	w := &RAWWriter{}
	w.writer = bufio.NewWriter(writeOpts.OutStream)
	w.delimiter, err = strconv.Unquote(`"` + writeOpts.OutDelimiter + `"`)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	w.outHeader = writeOpts.OutHeader
	if writeOpts.OutUseCRLF {
		w.endLine = "\r\n"
	} else {
		w.endLine = "\n"
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
func (w *RAWWriter) WriteRow(values []interface{}, _ []string) error {
	for n, col := range values {
		if n > 0 {
			if _, err := w.writer.WriteString(w.delimiter); err != nil {
				return err
			}
		}
		str := ""
		if col == nil && w.needNULL {
			str = w.outNULL
		} else {
			str = ValString(col)
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
