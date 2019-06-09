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
	w := &CSVWrite{}
	w.writer = csv.NewWriter(writeOpts.OutStream)
	w.writer.Comma, err = delimiter(writeOpts.OutDelimiter)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	w.outHeader = writeOpts.OutHeader
	return w
}

// PreWrite is output of header and preparation
func (w *CSVWrite) PreWrite(columns []string, types []string) error {
	if w.outHeader {
		err := w.writer.Write(columns)
		if err != nil {
			return err
		}
	}
	w.results = make([]string, len(columns))
	return nil
}

// WriteRow is row output
func (w *CSVWrite) WriteRow(values []interface{}, columns []string) error {
	for i, col := range values {
		w.results[i] = ValString(col)
	}
	err := w.writer.Write(w.results)
	return err
}

// PostWrite is flush
func (w *CSVWrite) PostWrite() error {
	w.writer.Flush()
	return nil
}
