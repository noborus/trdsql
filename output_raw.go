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
	if len(columns) == 0 {
		return nil
	}
	if err := w.writeColumn(columns[0]); err != nil {
		return err
	}
	for _, col := range columns[1:] {
		if _, err := w.writer.WriteString(w.delimiter); err != nil {
			return err
		}
		if err := w.writeColumn(col); err != nil {
			return err
		}
	}
	_, err := w.writer.WriteString(w.endLine)
	return err
}

// WriteRow is row write.
func (w *RAWWriter) WriteRow(values []any, _ []string) error {
	if len(values) == 0 {
		return nil
	}
	if err := w.writeColumn(values[0]); err != nil {
		return err
	}
	for _, col := range values[1:] {
		if _, err := w.writer.WriteString(w.delimiter); err != nil {
			return err
		}

		if err := w.writeColumn(col); err != nil {
			return err
		}
	}
	return w.writer.WriteByte('\n')
}

func (w *RAWWriter) writeColumn(value any) error {
	str := ValString(value)
	if value == nil && w.needNULL {
		str = w.outNULL
	}
	if _, err := w.writer.WriteString(str); err != nil {
		return err
	}
	return nil
}

// PostWrite is flush.
func (w *RAWWriter) PostWrite() error {
	return w.writer.Flush()
}
