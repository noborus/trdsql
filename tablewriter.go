package main

import (
	"github.com/olekukonko/tablewriter"
)

// TwOut is tablewriter struct
type TwOut struct {
	writer  *tablewriter.Table
	results []string
}

func (trdsql *TRDSQL) twOutNew() Output {
	tw := &TwOut{}
	tw.writer = tablewriter.NewWriter(trdsql.outStream)
	tw.writer.SetAutoFormatHeaders(false)
	if trdsql.omd {
		tw.writer.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		tw.writer.SetCenterSeparator("|")
	}
	return tw
}

func (tw *TwOut) first(columns []string) error {
	tw.writer.SetHeader(columns)
	tw.results = make([]string, len(columns))

	return nil
}

func (tw *TwOut) rowWrite(values []interface{}, columns []string) error {
	for i, col := range values {
		tw.results[i] = valString(col)
	}
	tw.writer.Append(tw.results)
	return nil
}

func (tw *TwOut) last() {
	tw.writer.Render()
}
