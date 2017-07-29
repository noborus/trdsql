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

func (trdsql TRDSQL) csvReader(db *DDB, sqlstr string, tablename string) (string, error) {
	var header []string
	reader, err := csvOpen(tablename, trdsql.inSep, trdsql.iskip)
	if err != nil {
		// no file
		return sqlstr, nil
	}
	rtable := db.escapetable(tablename)
	sqlstr = db.rewrite(sqlstr, tablename, rtable)
	header, err = csvheader(reader)
	if err != nil {
		log.Println(err)
		return sqlstr, err
	}
	db.Create(rtable, header, trdsql.ihead)
	err = db.ImportPrepare(rtable, header, trdsql.ihead)
	if err != nil {
		log.Println(err)
		return sqlstr, err
	}
	db.csvImport(reader, header, trdsql.ihead)
	return sqlstr, nil
}

func (trdsql TRDSQL) csvWrite(rows *sql.Rows) error {
	defer rows.Close()
	writer := csv.NewWriter(trdsql.outStream)
	var err error
	writer.Comma, err = getSeparator(trdsql.outSep)
	if err != nil {
		log.Println(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ERROR: Rows %s", err)
	}
	if trdsql.outHeader {
		writer.Write(columns)
	}

	results := make([]string, len(columns))
	err = write(rows, columns, func(values []interface{}) {
		for i, col := range values {
			results[i] = valString(col)
		}
		writer.Write(results)
	})
	writer.Flush()
	return err
}
