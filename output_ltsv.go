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

func NewLTSVWrite() *LTSVWrite {
	lw := &LTSVWrite{}
	lw.delimiter = "\t"
	lw.writer = bufio.NewWriter(DefaultWriteOpts.OutStream)
	return lw
}

// First is preparation
func (lw *LTSVWrite) First(columns []string, types []string) error {
	lw.results = make(map[string]string, len(columns))
	return nil
}

// WriteRow is Actual output
func (lw *LTSVWrite) WriteRow(values []interface{}, columns []string) error {
	results := make([]string, len(values))
	for i, col := range values {
		results[i] = columns[i] + ":" + ValString(col)
	}
	str := strings.Join(results, lw.delimiter) + "\n"
	_, err := lw.writer.Write([]byte(str))
	return err
}

// Last is flush
func (lw *LTSVWrite) Last() error {
	return lw.writer.Flush()
}
