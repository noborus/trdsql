package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
)

// CSVIn provides methods of the Input interface
type CSVIn struct {
	reader   *csv.Reader
	names    []string
	types    []string
	preRead  [][]string
	inHeader bool
}

// CSVOut provides methods of the Output interface
type CSVOut struct {
	writer    *csv.Writer
	results   []string
	outHeader bool
}

func delimiter(sepString string) (rune, error) {
	if sepString == "" {
		return 0, nil
	}
	sepRunes, err := strconv.Unquote(`'` + sepString + `'`)
	if err != nil {
		return ',', fmt.Errorf("can not get separator: %s:\"%s\"", err, sepString)
	}
	sepRune := ([]rune(sepRunes))[0]
	return sepRune, err
}

func (trdsql *TRDSQL) csvInputNew(r io.Reader) (Input, error) {
	var err error
	if trdsql.inHeader {
		trdsql.inPreRead--
	}
	cr := &CSVIn{}
	cr.reader = csv.NewReader(r)
	cr.reader.LazyQuotes = true
	cr.reader.FieldsPerRecord = -1 // no check count
	cr.reader.TrimLeadingSpace = true
	cr.inHeader = trdsql.inHeader
	cr.reader.Comma, err = delimiter(trdsql.inDelimiter)
	return cr, err
}

// GetColumn is reads the specified number of rows and determines the column name.
// The previously read row is stored in preRead.
func (cr *CSVIn) GetColumn(rowNum int) ([]string, error) {
	// Header
	if cr.inHeader {
		row, err := cr.reader.Read()
		if err != nil {
			return nil, err
		}
		cr.names = make([]string, len(row))
		for i, col := range row {
			if col == "" {
				cr.names[i] = "c" + strconv.Itoa(i+1)
			} else {
				cr.names[i] = col
			}
		}
	}

	for n := 0; n < rowNum; n++ {
		row, err := cr.reader.Read()
		if err != nil {
			return cr.names, err
		}
		rows := make([]string, len(row))
		for i, col := range row {
			rows[i] = col
			if len(cr.names) < i+1 {
				cr.names = append(cr.names, "c"+strconv.Itoa(i+1))
			}
		}
		cr.preRead = append(cr.preRead, rows)
	}
	return cr.names, nil
}

// GetTypes is reads the specified number of rows and determines the column type.
func (cr *CSVIn) GetTypes() ([]string, error) {
	cr.types = make([]string, len(cr.names))
	for i := 0; i < len(cr.names); i++ {
		cr.types[i] = "text"
	}
	return cr.types, nil
}

// PreReadRow is returns only columns that store preread rows.
func (cr *CSVIn) PreReadRow() [][]interface{} {
	rowNum := len(cr.preRead)
	rows := make([][]interface{}, rowNum)
	for n := 0; n < rowNum; n++ {
		rows[n] = make([]interface{}, len(cr.names))
		for i, f := range cr.preRead[n] {
			rows[n][i] = f
		}
	}
	return rows
}

// ReadRow is read the rest of the row.
func (cr *CSVIn) ReadRow(row []interface{}) ([]interface{}, error) {
	record, err := cr.reader.Read()
	if err != nil {
		return row, err
	}
	for i := 0; len(row) > i; i++ {
		if len(record) > i {
			row[i] = record[i]
		} else {
			row[i] = nil
		}
	}
	return row, nil
}

func (trdsql *TRDSQL) csvOutNew() Output {
	var err error
	c := &CSVOut{}
	c.writer = csv.NewWriter(trdsql.outStream)
	c.writer.Comma, err = delimiter(trdsql.outDelimiter)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	c.outHeader = trdsql.outHeader
	return c
}

// First is output of header and preparation
func (c *CSVOut) First(columns []string, types []string) error {
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
