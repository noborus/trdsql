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
	err = write(rows, columns, func(values []interface{}) {
		for i, col := range values {
			results[i] = valString(col)
		}
		fmt.Fprint(writer, strings.Join(results, sep), "\n")
	})
	writer.Flush()
	return err
}
