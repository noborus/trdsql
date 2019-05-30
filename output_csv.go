package trdsql

import (
	"encoding/csv"
)

// CSVOut provides methods of the Output interface
type CSVOut struct {
	writer    *csv.Writer
	results   []string
	outHeader bool
}

func (trdsql *TRDSQL) csvOutNew() *CSVOut {
	var err error
	c := &CSVOut{}
	c.writer = csv.NewWriter(trdsql.OutStream)
	c.writer.Comma, err = delimiter(trdsql.OutDelimiter)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	c.outHeader = trdsql.OutHeader
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

// WriteRow is row output
func (c *CSVOut) WriteRow(values []interface{}, columns []string) error {
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
