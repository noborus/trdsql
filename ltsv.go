package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"

	"github.com/najeira/ltsv"
)

func ltsvOpen(filename string, delimiter string, skip int) (*ltsv.Reader, error) {
	file, err := tFileOpen(filename)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	reader := ltsv.NewReader(file)
	reader.Delimiter, err = getSeparator(delimiter)
	if err != nil {
		return nil, err
	}
	for i := 0; i < skip; i++ {
		r, _ := reader.Read()
		debug.Printf("Skip row:%s\n", r)
	}
	return reader, nil
}

func (trdsql TRDSQL) ltsvReader(db *DDB, sqlstr string, tablename string) (string, error) {
	reader, err := ltsvOpen(tablename, "\t", trdsql.iskip)
	if err != nil {
		// no file
		return sqlstr, nil
	}
	rtable := db.escapetable(tablename)
	sqlstr = db.rewrite(sqlstr, tablename, rtable)
	first, err := reader.Read()
	if err != nil {
		return sqlstr, err
	}
	header := keys(first)
	db.Create(rtable, header, true)
	err = db.ImportPrepare(rtable, header, true)
	if err != nil {
		log.Println(err)
		return sqlstr, err
	}

	db.ltsvImport(reader, first, header)
	return sqlstr, nil
}

func keys(m map[string]string) []string {
	ks := []string{}
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func (db *DDB) ltsvImport(reader *ltsv.Reader, first map[string]string, header []string) error {
	list := make([]interface{}, len(header))
	for i := range header {
		list[i] = first[header[i]]
	}
	rowImport(db.stmt, list)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else {
			if err != nil {
				return fmt.Errorf("ERROR Read: %s", err)
			}
		}
		for i := range header {
			list[i] = record[header[i]]
		}
		rowImport(db.stmt, list)
	}
	return nil
}

func (trdsql TRDSQL) ltsvWrite(rows *sql.Rows) error {
	defer rows.Close()
	writer := ltsv.NewWriter(trdsql.outStream)
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ERROR: Rows %s", err)
	}

	results := make(map[string]string, len(columns))
	err = write(rows, columns, func(values []interface{}) {
		for i, col := range values {
			results[columns[i]] = valString(col)
		}
		writer.Write(results)
	})
	writer.Flush()
	return err
}
