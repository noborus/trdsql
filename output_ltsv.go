package trdsql

import (
	"bufio"
	"strings"
)

// LTSVWrite provides methods of the Output interface
type LTSVWrite struct {
	writer    *bufio.Writer
	delimiter string
	results   map[string]string
}

func NewLTSVWrite(writeOpts WriteOpts) *LTSVWrite {
	w := &LTSVWrite{}
	w.delimiter = "\t"
	w.writer = bufio.NewWriter(writeOpts.OutStream)
	return w
}

// PreWrite is preparation
func (w *LTSVWrite) PreWrite(columns []string, types []string) error {
	w.results = make(map[string]string, len(columns))
	return nil
}

// WriteRow is Actual output
func (w *LTSVWrite) WriteRow(values []interface{}, columns []string) error {
	results := make([]string, len(values))
	for i, col := range values {
		results[i] = columns[i] + ":" + ValString(col)
	}
	str := strings.Join(results, w.delimiter) + "\n"
	_, err := w.writer.Write([]byte(str))
	return err
}

// PostWrite is flush
func (w *LTSVWrite) PostWrite() error {
	return w.writer.Flush()
}
