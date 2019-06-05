package trdsql

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// RawWrite provides methods of the Output interface
type RawWrite struct {
	writer    *bufio.Writer
	results   []string
	sep       string
	outHeader bool
}

func (trdsql *TRDSQL) NewRAWWrite() *RawWrite {
	var err error
	raw := &RawWrite{}
	raw.writer = bufio.NewWriter(trdsql.OutStream)
	raw.sep, err = strconv.Unquote(`"` + trdsql.OutDelimiter + `"`)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	raw.outHeader = trdsql.OutHeader
	return raw
}

// First is output of header and preparation
func (raw *RawWrite) First(columns []string, types []string) error {
	if raw.outHeader {
		_, err := fmt.Fprint(raw.writer, strings.Join(columns, raw.sep), "\n")
		if err != nil {
			debug.Printf("%s\n", err)
		}
	}
	raw.results = make([]string, len(columns))
	return nil
}

// WriteRow is row output
func (raw *RawWrite) WriteRow(values []interface{}, columns []string) error {
	for i, col := range values {
		raw.results[i] = ValString(col)
	}
	_, err := fmt.Fprint(raw.writer, strings.Join(raw.results, raw.sep), "\n")
	if err != nil {
		debug.Printf("%s\n", err)
	}
	return nil
}

// Last is flush
func (raw *RawWrite) Last() error {
	return raw.writer.Flush()
}
