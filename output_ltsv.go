package trdsql

import (
	"bufio"
	"strings"
)

// LTSVWriter provides methods of the Writer interface.
type LTSVWriter struct {
	writer    *bufio.Writer
	delimiter string
	results   map[string]string
}

// NewLTSVWriter returns LTSVWriter.
func NewLTSVWriter(writeOpts WriteOpts) *LTSVWriter {
	w := &LTSVWriter{}
	w.delimiter = "\t"
	w.writer = bufio.NewWriter(writeOpts.OutStream)
	return w
}

// PreWrite is area preparation.
func (w *LTSVWriter) PreWrite(columns []string, types []string) error {
	w.results = make(map[string]string, len(columns))
	return nil
}

// WriteRow is row write.
func (w *LTSVWriter) WriteRow(values []interface{}, columns []string) error {
	results := make([]string, len(values))
	for i, col := range values {
		results[i] = columns[i] + ":" + ValString(col)
	}
	str := strings.Join(results, w.delimiter) + "\n"
	_, err := w.writer.Write([]byte(str))
	return err
}

// PostWrite is flush.
func (w *LTSVWriter) PostWrite() error {
	return w.writer.Flush()
}
