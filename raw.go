package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

func (trdsql TRDSQL) rawWrite(rows *sql.Rows) error {
	defer rows.Close()
	writer := bufio.NewWriter(trdsql.outStream)
	sep, err := strconv.Unquote(`"` + trdsql.outSep + `"`)
	if err != nil {
		return fmt.Errorf("ERROR: Delimiter [%s]:%s", trdsql.outSep, err)
	}
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ERROR: Rows %s", err)
	}
	if trdsql.outHeader {
		fmt.Fprint(writer, strings.Join(columns, sep), "\n")
	}

	results := make([]string, len(columns))
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
		for i, col := range values {
			results[i] = valString(col)
		}
		fmt.Fprint(writer, strings.Join(results, sep), "\n")
	}
	writer.Flush()
	return nil
}
