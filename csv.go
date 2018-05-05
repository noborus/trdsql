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
	name     []string
	firstRow []string
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
	cr.reader.LazyQuotes = true
	cr.reader.FieldsPerRecord = -1 // no check count
	cr.reader.TrimLeadingSpace = true
	cr.reader.Comma, err = separator(trdsql.inSep)
	cr.inHeader = trdsql.inHeader
	return cr, err
}

// FirstRead is read input to determine column of table
func (cr *CSVIn) FirstRead() ([]string, error) {
	first, err := cr.reader.Read()
	if err != nil {
		return nil, err
	}
	cr.name = make([]string, len(first))
	cr.firstRow = make([]string, len(first))
	for i, row := range first {
		if cr.inHeader {
			if row == "" {
				cr.name[i] = "c" + strconv.Itoa(i+1)
			} else {
				cr.name[i] = row
			}
		} else {
			cr.name[i] = "c" + strconv.Itoa(i+1)
			cr.firstRow[i] = row
		}
	}
	debug.Printf("Column Name: [%v]", strings.Join(cr.name, ","))
	return cr.name, err
}

// FirstRowRead is read the first row
func (cr *CSVIn) FirstRowRead(list []interface{}) []interface{} {
	for i, f := range cr.firstRow {
		list[i] = f
	}
	return list
}

// RowRead is read 2row or later
func (cr *CSVIn) RowRead(list []interface{}) ([]interface{}, error) {
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
	c.writer.Comma, err = separator(trdsql.outSep)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	c.outHeader = trdsql.outHeader
	return c
}

// First is output of header and preparation
func (c *CSVOut) First(columns []string) error {
	if c.outHeader {
		err := c.writer.Write(columns)
		if err != nil {
			return err
		}
	}
	c.results = make([]string, len(columns))
	return nil
}

// RowWrite is row output
func (c *CSVOut) RowWrite(values []interface{}, columns []string) error {
	for i, col := range values {
		c.results[i] = valString(col)
	}
	err := c.writer.Write(c.results)
	return err
}

// Last is flush
func (c *CSVOut) Last() error {
	c.writer.Flush()
	return nil
}
