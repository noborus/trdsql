package main

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

// LTSVIn provides methods of the Input interface
type LTSVIn struct {
	reader    *bufio.Reader
	frow      map[string]string
	delimiter string
	header    []string
}

// LTSVOut provides methods of the Output interface
type LTSVOut struct {
	writer    *bufio.Writer
	delimiter string
	results   map[string]string
}

func (trdsql *TRDSQL) ltsvInputNew(r io.Reader) (Input, error) {
	lr := &LTSVIn{}
	lr.reader = bufio.NewReader(r)
	lr.delimiter = "\t"
	return lr, nil
}

func (lr *LTSVIn) read() (map[string]string, []string, error) {
	line, _, err := lr.reader.ReadLine()
	if err != nil {
		return nil, nil, err
	}
	tline := strings.TrimSpace(string(line))
	if len(tline) == 0 {
		return nil, nil, errors.New("no line")
	}
	columns := strings.Split(tline, lr.delimiter)
	lvs := make(map[string]string)
	keys := make([]string, 0, len(columns))
	for _, column := range columns {
		data := strings.SplitN(column, ":", 2)
		if len(data) != 2 {
			return nil, nil, errors.New("LTSV format error")
		}
		lvs[data[0]] = data[1]
		keys = append(keys, data[0])
	}
	return lvs, keys, nil
}

func (lr *LTSVIn) firstRead() ([]string, error) {
	var err error
	lr.frow, lr.header, err = lr.read()
	if err != nil {
		return nil, err
	}
	debug.Printf("Column Name: [%v]", strings.Join(lr.header, ","))
	return lr.header, nil
}

func (lr *LTSVIn) firstRow(list []interface{}) []interface{} {
	for i := range lr.header {
		list[i] = lr.frow[lr.header[i]]
	}
	return list
}

func (lr *LTSVIn) rowRead(list []interface{}) ([]interface{}, error) {
	record, _, err := lr.read()
	if err != nil {
		return list, err
	}
	for i := range lr.header {
		list[i] = record[lr.header[i]]
	}
	return list, nil
}

func (trdsql *TRDSQL) ltsvOutNew() Output {
	lw := &LTSVOut{}
	lw.delimiter = "\t"
	lw.writer = bufio.NewWriter(trdsql.outStream)
	return lw
}

func (lw *LTSVOut) first(columns []string) error {
	lw.results = make(map[string]string, len(columns))
	return nil
}

func (lw *LTSVOut) rowWrite(values []interface{}, columns []string) error {
	results := make([]string, len(values))
	for i, col := range values {
		results[i] = columns[i] + ":" + valString(col)
	}
	str := strings.Join(results, lw.delimiter) + "\n"
	lw.writer.Write([]byte(str))
	return nil
}

func (lw *LTSVOut) last() {
	lw.writer.Flush()
}
