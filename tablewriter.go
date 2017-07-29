package main

import (
	"database/sql"
	"fmt"

	"github.com/olekukonko/tablewriter"
)

func (trdsql TRDSQL) twWrite(rows *sql.Rows) error {
	defer rows.Close()
	writer := tablewriter.NewWriter(trdsql.outStream)
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ERROR: Rows %s", err)
	}
	writer.SetHeader(columns)
	if trdsql.omd {
		writer.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		writer.SetCenterSeparator("|")
	}

	results := make([]string, len(columns))
	err = write(rows, columns, func(values []interface{}) {
		for i, col := range values {
			results[i] = valString(col)
		}
		writer.Append(results)
	})
	writer.Render()
	return err
}
