package main

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
	raw.writer = bufio.NewWriter(trdsql.outStream)
	raw.sep, err = strconv.Unquote(`"` + trdsql.outSep + `"`)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	raw.outHeader = trdsql.outHeader
	return raw
}

func (raw *RawOut) First(columns []string) error {
	if raw.outHeader {
		fmt.Fprint(raw.writer, strings.Join(columns, raw.sep), "\n")
	}
	raw.results = make([]string, len(columns))
	return nil
}

func (raw *RawOut) RowWrite(values []interface{}, columns []string) error {
	for i, col := range values {
		raw.results[i] = valString(col)
	}
	fmt.Fprint(raw.writer, strings.Join(raw.results, raw.sep), "\n")
	return nil
}

func (raw *RawOut) Last() error {
	return raw.writer.Flush()
}
