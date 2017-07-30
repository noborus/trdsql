package main

import (
	"encoding/csv"
	"os"
)

// CSVIn provides methods of the Input interface
type CSVIn struct {
	reader *csv.Reader
	header []string
	ihead  bool
}

// CSVOut provides methods of the Output interface
type CSVOut struct {
	writer    *csv.Writer
	results   []string
	outHeader bool
}

func (trdsql TRDSQL) csvInputNew(file *os.File) (Input, error) {
	var err error
	cr := &CSVIn{}
	cr.reader = csv.NewReader(file)
	cr.reader.FieldsPerRecord = -1 // no check count
	cr.reader.TrimLeadingSpace = true
	cr.ihead = trdsql.ihead
	cr.reader.Comma, err = getSeparator(trdsql.inSep)
	if err != nil {
		return cr, err
	}
	return cr, err
}

func (cr *CSVIn) firstRead(tablename string) ([]string, error) {
	var err error
	cr.header, err = cr.reader.Read()
	if err != nil {
		return nil, err
	}
	return cr.header, err
}

func (cr *CSVIn) firstRow(list []interface{}) []interface{} {
	for i := range cr.header {
		list[i] = cr.header[i]
	}
	return list
}

func (cr *CSVIn) rowRead(list []interface{}) ([]interface{}, error) {
	record, err := cr.reader.Read()
	if err != nil {
		return list, err
	}
	for i := 0; len(list) > i; i++ {
		if len(record) > i {
			list[i] = record[i]
		} else {
			list[i] = nil
		}
	}
	return list, nil
}

func (trdsql TRDSQL) csvOutNew() Output {
	var err error
	c := &CSVOut{}
	c.writer = csv.NewWriter(trdsql.outStream)
	c.writer.Comma, err = getSeparator(trdsql.outSep)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	c.outHeader = trdsql.outHeader
	return c
}

func (c *CSVOut) first(scanArgs []interface{}, columns []string) error {
	if c.outHeader {
		c.writer.Write(columns)
	}
	c.results = make([]string, len(columns))
	return nil
}

func (c *CSVOut) rowWrite(values []interface{}, columns []string) error {
	for i, col := range values {
		c.results[i] = valString(col)
	}
	c.writer.Write(c.results)
	return nil
}

func (c *CSVOut) last() {
	c.writer.Flush()
}
