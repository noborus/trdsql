package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strings"
)

func (trdsql TRDSQL) csvRead(db *DDB, sqlstr string, tablename string) (string, error) {
	var header []string
	reader, err := csvOpen(tablename, trdsql.inSep, trdsql.iskip)
	if err != nil {
		// no file
		return sqlstr, nil
	}
	rtable := db.escapetable(tablename)
	sqlstr = db.rewrite(sqlstr, tablename, rtable)
	header, err = reader.Read()
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

func (db *DDB) csvImport(reader *csv.Reader, header []string, head bool) error {
	list := make([]interface{}, len(header))
	for i := range header {
		list[i] = header[i]
	}
	if !head {
		rowImport(db.stmt, list)
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else {
			if err != nil {
				return fmt.Errorf("ERROR Read: %s", err)
			}
		}
		for i := 0; len(list) > i && len(record) > i; i++ {
			list[i] = record[i]
		}
		rowImport(db.stmt, list)
	}
	return nil
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
