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
	preRead   []map[string]string
	delimiter string
	names     []string
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

// GetColumn is read input to determine column of table
func (lr *LTSVIn) GetColumn(rowNum int) ([]string, error) {
	names := map[string]bool{}
	for i := 0; i < rowNum; i++ {
		rows, keys, err := lr.read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		for k := 0; k < len(keys); k++ {
			if !names[keys[k]] {
				names[keys[k]] = true
				lr.names = append(lr.names, keys[k])
			}
		}
		lr.preRead = append(lr.preRead, rows)
	}
	debug.Printf("Column Names: [%v]", strings.Join(lr.names, ","))
	return lr.names, nil
}

// PreReadRow is read the first row
func (lr *LTSVIn) PreReadRow() [][]interface{} {
	rowNum := len(lr.preRead)
	rows := make([][]interface{}, rowNum)
	for n := 0; n < rowNum; n++ {
		rows[n] = make([]interface{}, len(lr.names))
		for i := range lr.names {
			rows[n][i] = lr.preRead[n][lr.names[i]]
		}
	}
	return rows
}

// ReadRow is read 2row or later
func (lr *LTSVIn) ReadRow(row []interface{}) ([]interface{}, error) {
	record, _, err := lr.read()
	if err != nil {
		return row, err
	}
	for i := range lr.names {
		row[i] = record[lr.names[i]]
	}
	return row, nil
}

func (lr *LTSVIn) read() (map[string]string, []string, error) {
	line, err := lr.readline()
	if err != nil {
		return nil, nil, err
	}
	columns := strings.Split(line, lr.delimiter)
	lvs := make(map[string]string)
	keys := make([]string, 0, len(columns))
	for _, column := range columns {
		kv := strings.SplitN(column, ":", 2)
		if len(kv) != 2 {
			return nil, nil, errors.New("LTSV format error")
		}
		lvs[kv[0]] = kv[1]
		keys = append(keys, kv[0])
	}
	return lvs, keys, nil
}

func (lr *LTSVIn) readline() (string, error) {
	for {
		line, _, err := lr.reader.ReadLine()
		if err != nil {
			return "", err
		}
		tline := strings.TrimSpace(string(line))
		if len(tline) != 0 {
			return tline, nil
		}
	}
}

func (trdsql *TRDSQL) ltsvOutNew() Output {
	lw := &LTSVOut{}
	lw.delimiter = "\t"
	lw.writer = bufio.NewWriter(trdsql.outStream)
	return lw
}

// First is preparation
func (lw *LTSVOut) First(columns []string) error {
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
