package trdsql

import (
	"bufio"
)

// LTSVWriter provides methods of the Writer interface.
type LTSVWriter struct {
	writer    *bufio.Writer
	delimiter rune
	results   []string
	needNULL  bool
	outNULL   string
}

// NewLTSVWriter returns LTSVWriter.
func NewLTSVWriter(writeOpts *WriteOpts) *LTSVWriter {
	w := &LTSVWriter{}
	w.delimiter = '\t'
	w.writer = bufio.NewWriter(writeOpts.OutStream)
	w.needNULL = writeOpts.OutNeedNULL
	w.outNULL = writeOpts.OutNULL
	return w
}

// PreWrite is area preparation.
func (w *LTSVWriter) PreWrite(columns []string, types []string) error {
	w.results = make([]string, len(columns))
	return nil
}

// WriteRow is row write.
func (w *LTSVWriter) WriteRow(values []interface{}, labels []string) error {
	for n, col := range values {
		if n > 0 {
			if _, err := w.writer.WriteRune(w.delimiter); err != nil {
				return err
			}
		}
		if _, err := w.writer.WriteString(labels[n]); err != nil {
			return err
		}
		if err := w.writer.WriteByte(':'); err != nil {
			return err
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
func (w *LTSVWriter) PostWrite() error {
	return w.writer.Flush()
}
