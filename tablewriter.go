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
	err = db.twRowsWrite(writer, rows)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}

func (db *DDB) twRowsWrite(writer *tablewriter.Table, rows *sql.Rows) error {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ERROR: Rows %s", err)
	}
	values := make([]interface{}, len(columns))
	results := make([]string, len(columns))
	scanArgs := make([]interface{}, len(columns))
	writer.SetHeader(columns)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("ERROR: %s", err)
		}
		for i, col := range values {
			b, ok := col.([]byte)
			if ok {
				results[i] = string(b)
			} else {
				results[i] = fmt.Sprint(col)
			}
		}
		writer.Append(results)
	}
	writer.Render()

	return nil
}
