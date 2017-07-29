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
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("ERROR: %s", err)
		}
		m := make(map[string]string, len(columns))
		for i, col := range values {
			m[columns[i]] = valString(col)
		}
		results = append(results, m)
	}
	writer.Encode(results)
	return nil
}
