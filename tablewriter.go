package main

import (
	"github.com/olekukonko/tablewriter"
)

// TwOut is tablewriter struct
type TwOut struct {
	writer  *tablewriter.Table
	results []string
}

func (trdsql *TRDSQL) twOutNew(markdown bool) Output {
	tw := &TwOut{}
	tw.writer = tablewriter.NewWriter(trdsql.outStream)
	tw.writer.SetAutoFormatHeaders(false)
	if markdown {
		tw.writer.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		tw.writer.SetCenterSeparator("|")
	}
	return tw
}

// First is preparation
func (tw *TwOut) First(columns []string, types []string) error {
	tw.writer.SetHeader(columns)
	tw.results = make([]string, len(columns))

	return nil
}

// RowWrite is Addition to array
func (tw *TwOut) RowWrite(values []interface{}, columns []string) error {
	for i, col := range values {
		tw.results[i] = valString(col)
	}
	tw.writer.Append(tw.results)
	return nil
}

// Last is Actual output
func (tw *TwOut) Last() error {
	tw.writer.Render()
	return nil
}
