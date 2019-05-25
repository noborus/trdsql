package trdsql

import (
	"bufio"
	"strings"
)

// LTSVOut provides methods of the Output interface
type LTSVOut struct {
	writer    *bufio.Writer
	delimiter string
	results   map[string]string
}

func (trdsql *TRDSQL) ltsvOutNew() Output {
	lw := &LTSVOut{}
	lw.delimiter = "\t"
	lw.writer = bufio.NewWriter(trdsql.OutStream)
	return lw
}

// First is preparation
func (lw *LTSVOut) First(columns []string, types []string) error {
	lw.results = make(map[string]string, len(columns))
	return nil
}

// RowWrite is Actual output
func (lw *LTSVOut) RowWrite(values []interface{}, columns []string) error {
	results := make([]string, len(values))
	for i, col := range values {
		results[i] = columns[i] + ":" + valString(col)
	}
	str := strings.Join(results, lw.delimiter) + "\n"
	_, err := lw.writer.Write([]byte(str))
	return err
}

// Last is flush
func (lw *LTSVOut) Last() error {
	return lw.writer.Flush()
}
