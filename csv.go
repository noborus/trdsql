package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"strings"
)

func csvOpen(filename string, delimiter string, skip int) (*csv.Reader, error) {

	file, err := tFileOpen(filename)
	if err != nil {
		return nil, err
	}
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // no check count
	reader.TrimLeadingSpace = true
	reader.Comma, err = getSeparator(delimiter)
	if err != nil {
		return nil, err
	}
	for i := 0; i < skip; i++ {
		r, _ := reader.Read()
		debug.Printf("Skip row:%s\n", strings.Join(r, " "))
	}
	return reader, err
}

func csvheader(reader *csv.Reader) ([]string, error) {
	var err error
	var header []string
	header, err = reader.Read()
	return header, err
}

func (trdsql TRDSQL) csvReader(db *DDB, sqlstr string, tablename string) (string, int) {
	var header []string
	reader, err := csvOpen(tablename, trdsql.inSep, trdsql.iskip)
	if err != nil {
		// no file
		return sqlstr, 0
	}
	rtable := db.escapetable(tablename)
	sqlstr = db.rewrite(sqlstr, tablename, rtable)
	header, err = csvheader(reader)
	if err != nil {
		log.Println(err)
		return sqlstr, 1
	}
	db.Create(rtable, header, trdsql.ihead)
	err = db.ImportPrepare(rtable, header, trdsql.ihead)
	if err != nil {
		log.Println(err)
		return sqlstr, 1
	}
	db.csvImport(reader, header, trdsql.ihead)
	return sqlstr, 0
}

func (trdsql TRDSQL) csvWrite(db *DDB, sqlstr string) int {
	var err error
	writer := csv.NewWriter(trdsql.outStream)
	writer.Comma, err = getSeparator(trdsql.outSep)
	if err != nil {
		log.Println(err)
	}
	rows, err := db.Select(sqlstr)
	if err != nil {
		log.Println(err)
		return 1
	}
	err = db.csvRowsWrite(writer, rows, trdsql.outHeader)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}

func (db *DDB) csvRowsWrite(writer *csv.Writer, rows *sql.Rows, head bool) error {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ERROR: Rows %s", err)
	}
	if head {
		writer.Write(columns)
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
		writer.Write(results)
	}
	writer.Flush()
	return nil
}
