package main

import (
	"os"

	"github.com/najeira/ltsv"
)

// LTSVIn provides methods of the Input interface
type LTSVIn struct {
	reader   *ltsv.Reader
	firstrow map[string]string
	header   []string
}

// LTSVOut provides methods of the Output interface
type LTSVOut struct {
	writer  *ltsv.Writer
	results map[string]string
}

func (trdsql TRDSQL) ltsvInputNew(file *os.File) (Input, error) {
	var err error
	lr := &LTSVIn{}
	lr.reader = ltsv.NewReader(file)
	lr.reader.Delimiter, err = getSeparator("\t")
	if err != nil {
		return nil, err
	}
	return lr, nil
}

func (lr *LTSVIn) firstRead(tablename string) ([]string, error) {
	var err error
	lr.firstrow, err = lr.reader.Read()
	if err != nil {
		return nil, err
	}
	lr.header = keys(lr.firstrow)
	return lr.header, nil
}

func (lr *LTSVIn) firstRow(list []interface{}) []interface{} {
	for i := range lr.header {
		list[i] = lr.firstrow[lr.header[i]]
	}
	return list
}

func (lr *LTSVIn) rowRead(list []interface{}) ([]interface{}, error) {
	record, err := lr.reader.Read()
	if err != nil {
		return list, err
	}
	for i := range lr.header {
		list[i] = record[lr.header[i]]
	}

	return list, nil
}

func keys(m map[string]string) []string {
	ks := []string{}
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func (trdsql TRDSQL) ltsvOutNew() Output {
	lw := &LTSVOut{}
	lw.writer = ltsv.NewWriter(trdsql.outStream)
	return lw
}

func (lw *LTSVOut) first(scanArgs []interface{}, columns []string) error {
	lw.results = make(map[string]string, len(columns))
	return nil
}

func (lw *LTSVOut) rowWrite(values []interface{}, columns []string) error {
	for i, col := range values {
		lw.results[columns[i]] = valString(col)
	}
	lw.writer.Write(lw.results)
	return nil
}

func (lw *LTSVOut) last() {
	lw.writer.Flush()
}
