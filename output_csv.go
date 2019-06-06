package trdsql

import (
	"encoding/csv"
)

// CSVWrite provides methods of the Output interface
type CSVWrite struct {
	writer    *csv.Writer
	results   []string
	outHeader bool
}

func NewCSVWrite(writeOpts WriteOpts) *CSVWrite {
	var err error
	c := &CSVWrite{}
	c.writer = csv.NewWriter(writeOpts.OutStream)
	c.writer.Comma, err = delimiter(writeOpts.OutDelimiter)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	c.outHeader = writeOpts.OutHeader
	return c
}

// First is output of header and preparation
func (c *CSVWrite) First(columns []string, types []string) error {
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
func (c *CSVWrite) WriteRow(values []interface{}, columns []string) error {
	for i, col := range values {
		c.results[i] = ValString(col)
	}
	err := c.writer.Write(c.results)
	return err
}

// Last is flush
func (c *CSVWrite) Last() error {
	c.writer.Flush()
	return nil
}
