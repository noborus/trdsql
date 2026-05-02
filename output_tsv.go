package trdsql

import (
	"bufio"
	"strings"
)

// TSVWriter provides methods of the Writer interface.
// TSV (IANA text/tab-separated-values) has no quoting or escaping.
// Fields containing tab or newline characters have those replaced with spaces.
type TSVWriter struct {
	writer    *bufio.Writer
	replacer  *strings.Replacer
	endLine   string
	outNULL   string
	outHeader bool
	needNULL  bool
}

// NewTSVWriter returns TSVWriter.
func NewTSVWriter(writeOpts *WriteOpts) *TSVWriter {
	w := &TSVWriter{}
	w.writer = bufio.NewWriter(writeOpts.OutStream)
	w.replacer = strings.NewReplacer("\t", " ", "\n", " ")
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
func (w *TSVWriter) PreWrite(columns, types []string) error {
	if !w.outHeader {
		return nil
	}
	for n, column := range columns {
		if n > 0 {
			if err := w.writer.WriteByte('\t'); err != nil {
				return err
			}
		}
		if _, err := w.writer.WriteString(w.replacer.Replace(column)); err != nil {
			return err
		}
	}
	_, err := w.writer.WriteString(w.endLine)
	return err
}

// WriteRow is row write.
func (w *TSVWriter) WriteRow(values []any, _ []string) error {
	for n, col := range values {
		if n > 0 {
			if err := w.writer.WriteByte('\t'); err != nil {
				return err
			}
		}
		str := ""
		if col == nil {
			if w.needNULL {
				str = w.outNULL
			}
		} else {
			str = ValString(col)
		}
		if _, err := w.writer.WriteString(w.replacer.Replace(str)); err != nil {
			return err
		}
	}
	_, err := w.writer.WriteString(w.endLine)
	return err
}

// PostWrite is flush.
func (w *TSVWriter) PostWrite() error {
	return w.writer.Flush()
}
