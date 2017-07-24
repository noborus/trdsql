package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/olekukonko/tablewriter"
)

func (trdsql TRDSQL) twWrite(db *DDB, sqlstr string) int {
	writer := tablewriter.NewWriter(trdsql.outStream)
	rows, err := db.Select(sqlstr)
	if err != nil {
		log.Println(err)
		return 1
	}
	err = db.twRowsWrite(writer, rows, trdsql.omd)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}

func (db *DDB) twRowsWrite(writer *tablewriter.Table, rows *sql.Rows, omd bool) error {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ERROR: Rows %s", err)
	}
	writer.SetHeader(columns)
	if omd {
		writer.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		writer.SetCenterSeparator("|")
	}
	values := make([]interface{}, len(columns))
	results := make([]string, len(columns))
	scanArgs := make([]interface{}, len(columns))

	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("ERROR: %s", err)
		}
		for i, col := range values {
			results[i] = valString(col)
		}
		writer.Append(results)
	}
	writer.Render()

	return nil
}
