package trdsql

import (
	"encoding/csv"
)

func (trdsql *TRDSQL) csvOutNew() Output {
	var err error
	c := &CSVOut{}
	c.writer = csv.NewWriter(trdsql.OutStream)
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
