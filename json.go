package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
)

func (trdsql TRDSQL) jsonWrite(db *DDB, sqlstr string) int {
	writer := json.NewEncoder(trdsql.outStream)
	rows, err := db.Select(sqlstr)
	if err != nil {
		log.Println(err)
		return 1
	}
	err = db.jsonRowsWrite(writer, rows)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}

func (db *DDB) jsonRowsWrite(writer *json.Encoder, rows *sql.Rows) error {
	defer rows.Close()
	writer.SetIndent("", "  ")
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ERROR: Rows %s", err)
	}
	values := make([]interface{}, len(columns))
	tableData := make([]map[string]string, 0)
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("ERROR: %s", err)
		}
		results := make(map[string]string, len(columns))
		for i, col := range values {
			results[columns[i]] = valString(col)
		}
		tableData = append(tableData, results)
	}
	writer.Encode(tableData)
	return nil
}
