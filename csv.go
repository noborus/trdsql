package main

import (
	"encoding/csv"
	"io"
	"strconv"
	"strings"
)

// CSVIn provides methods of the Input interface
type CSVIn struct {
	reader   *csv.Reader
	header   []string
	fRow     []string
	inHeader bool
}

// CSVOut provides methods of the Output interface
type CSVOut struct {
	writer    *csv.Writer
	results   []string
	outHeader bool
}

func (trdsql *TRDSQL) csvInputNew(r io.Reader) (Input, error) {
	var err error
	cr := &CSVIn{}
	cr.reader = csv.NewReader(r)
	cr.reader.FieldsPerRecord = -1 // no check count
	cr.reader.TrimLeadingSpace = true
	cr.reader.Comma, err = getSeparator(trdsql.inSep)
	cr.inHeader = trdsql.ihead
	return cr, err
}

func (cr *CSVIn) firstRead(tablename string) ([]string, error) {
	first, err := cr.reader.Read()
	if err != nil {
		return nil, err
	}
	cr.header = make([]string, len(first))
	cr.fRow = make([]string, len(first))
	for i, row := range first {
		if cr.inHeader {
			if row == "" {
				cr.header[i] = "c" + strconv.Itoa(i+1)
			} else {
				cr.header[i] = row
			}
		} else {
			cr.header[i] = "c" + strconv.Itoa(i+1)
			cr.fRow[i] = row
		}
	}
	debug.Printf("Column Name: [%v]", strings.Join(cr.header, ","))
	return cr.header, err
}

func (cr *CSVIn) firstRow(list []interface{}) []interface{} {
	for i, f := range cr.fRow {
		list[i] = f
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

func (trdsql *TRDSQL) csvOutNew() Output {
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

func (c *CSVOut) first(columns []string) error {
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
