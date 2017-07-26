package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
)

func (trdsql TRDSQL) rawWrite(db *DDB, sqlstr string) int {
	var err error
	writer := bufio.NewWriter(trdsql.outStream)
	rows, err := db.Select(sqlstr)
	if err != nil {
		log.Println(err)
		return 1
	}
	err = db.rawRowsWrite(writer, trdsql.outSep, rows, trdsql.outHeader)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}

func (db *DDB) rawRowsWrite(writer *bufio.Writer, delimiter string, rows *sql.Rows, head bool) error {
	defer rows.Close()
	sep, err := strconv.Unquote(`"` + delimiter + `"`)
	if err != nil {
		return fmt.Errorf("ERROR: Delimiter [%s]:%s", delimiter, err)
	}
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ERROR: Rows %s", err)
	}
	if head {
		fmt.Fprint(writer, strings.Join(columns, sep), "\n")
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
		fmt.Fprint(writer, strings.Join(results, sep), "\n")
	}
	writer.Flush()
	return nil
}
