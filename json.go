package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

func (trdsql TRDSQL) jsonWrite(rows *sql.Rows) error {
	defer rows.Close()
	writer := json.NewEncoder(trdsql.outStream)
	writer.SetIndent("", "  ")
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ERROR: Rows %s", err)
	}

	results := make([]map[string]string, 0)
	err = write(rows, columns, func(values []interface{}) {
		m := make(map[string]string, len(columns))
		for i, col := range values {
			m[columns[i]] = valString(col)
		}
		results = append(results, m)
	})
	if err != nil {
		return err
	}
	err = writer.Encode(results)
	return err
}
