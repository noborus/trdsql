package trdsql

import (
	"bufio"
)

// LTSVWriter provides methods of the Writer interface.
type LTSVWriter struct {
	writer    *bufio.Writer
	outNULL   string
	results   []string
	delimiter rune
	needNULL  bool
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

// WriteRow is row write to LTSV.
func (w *LTSVWriter) WriteRow(values []any, labels []string) error {
	if len(values) == 0 {
		return nil
	}
	if err := w.writeColumn(labels[0], values[0]); err != nil {
		return err
	}
	for n, col := range values[1:] {
		if _, err := w.writer.WriteRune(w.delimiter); err != nil {
			return err
		}
		if err := w.writeColumn(labels[n+1], col); err != nil {
			return err
		}
	}
	return w.writer.WriteByte('\n')
}

func (w *LTSVWriter) writeColumn(label string, value any) error {
	if _, err := w.writer.WriteString(label); err != nil {
		return err
	}
	if err := w.writer.WriteByte(':'); err != nil {
		return err
	}

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
func (w *LTSVWriter) PostWrite() error {
	return w.writer.Flush()
}
