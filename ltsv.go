package main

import (
	"database/sql"
	"fmt"
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

func (trdsql TRDSQL) ltsvReader(db *DDB, sqlstr string, tablenames []string) (string, int) {
	for _, tablename := range tablenames {
		reader, err := ltsvOpen(tablename, trdsql.inSep, trdsql.iskip)
		if err != nil {
			// no file
			continue
		}
		rtable := db.escapetable(tablename)
		sqlstr = rewrite(sqlstr, tablename, rtable)
		first, err := reader.Read()
		if err != nil {
			return sqlstr, 1
		}
		header := keys(first)
		db.Create(rtable, header, true)
		err = db.ImportPrepare(rtable, header, true)
		if err != nil {
			log.Println(err)
			return sqlstr, 1
		}

		db.ltsvImport(reader, first, header)
	}
	return sqlstr, 0
}

func keys(m map[string]string) []string {
	ks := []string{}
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func (trdsql TRDSQL) ltsvWrite(db *DDB, sqlstr string) int {
	writer := ltsv.NewWriter(trdsql.outStream)
	rows, err := db.Select(sqlstr)
	if err != nil {
		log.Println(err)
		return 1
	}
	err = db.ltsvRowsWrite(writer, rows)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}

func (db *DDB) ltsvRowsWrite(writer *ltsv.Writer, rows *sql.Rows) error {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ERROR: Rows %s", err)
	}
	values := make([]interface{}, len(columns))
	results := make(map[string]string, len(columns))
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
			b, ok := col.([]byte)
			if ok {
				results[columns[i]] = string(b)
			} else {
				results[columns[i]] = fmt.Sprint(col)
			}
		}
		writer.Write(results)
	}
	writer.Flush()
	return nil
}
