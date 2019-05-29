package trdsql

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// RawOut provides methods of the Output interface
type RawOut struct {
	writer    *bufio.Writer
	results   []string
	sep       string
	outHeader bool
}

func (trdsql *TRDSQL) rawOutNew() Output {
	var err error
	raw := &RawOut{}
	raw.writer = bufio.NewWriter(trdsql.OutStream)
	raw.sep, err = strconv.Unquote(`"` + trdsql.outDelimiter + `"`)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	raw.outHeader = trdsql.outHeader
	return raw
}

// First is output of header and preparation
func (raw *RawOut) First(columns []string, types []string) error {
	if raw.outHeader {
		_, err := fmt.Fprint(raw.writer, strings.Join(columns, raw.sep), "\n")
		if err != nil {
			debug.Printf("%s\n", err)
		}
	}
	raw.results = make([]string, len(columns))
	return nil
}

// RowWrite is row output
func (raw *RawOut) RowWrite(values []interface{}, columns []string) error {
	for i, col := range values {
		raw.results[i] = valString(col)
	}
	_, err := fmt.Fprint(raw.writer, strings.Join(raw.results, raw.sep), "\n")
	if err != nil {
		debug.Printf("%s\n", err)
	}
	return nil
}

// Last is flush
func (raw *RawOut) Last() error {
	return raw.writer.Flush()
}
