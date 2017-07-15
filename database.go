package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

// DDB is *sql.DB wrapper.
type DDB struct {
	dbdriver string
	dbdsn    string
	*sql.DB
}

func rowImport(stmt *sql.Stmt, list []interface{}) {
	_, err := stmt.Exec(list...)
	if err != nil {
		log.Println(err)
	}
}

func (db DDB) Import(reader *csv.Reader, table string, header []string) error {
	columns := make([]string, len(header))
	place := make([]string, len(header))
	list := make([]interface{}, len(header))
	for i := range header {
		columns[i] = "c" + strconv.Itoa(i+1)
		if db.dbdriver == "postgres" {
			place[i] = "$" + strconv.Itoa(i+1)
		} else {
			place[i] = "?"
		}
		list[i] = header[i]
	}
	sqlstr := "INSERT INTO " + table + " (" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(place, ",") + ");"
	stmt, err := db.Prepare(sqlstr)
	if err != nil {
		return fmt.Errorf("ERROR INSERT: %s", err)
	}
	rowImport(stmt, list)

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
			list[i] = record[i]
		}
		rowImport(stmt, list)
	}
	return nil
}

func Connect(driver, dsn string) (DDB, error) {
	var db DDB
	var err error
	db.dbdriver = driver
	db.dbdsn = dsn
	db.DB, err = sql.Open(driver, dsn)
	return db, err
}

func (db DDB) Disconnect() error {
	err := db.Close()
	return err
}

func (db DDB) Create(table string, header []string) error {
	var sqlstr string
	columns := make([]string, len(header))
	for i := 0; i < len(header); i++ {
		columns[i] = "c" + strconv.Itoa(i+1) + " text"
		// columns[i] = "\"" + header[i] + "\"" + " text"
	}
	sqlstr = "CREATE TEMPORARY TABLE "
	sqlstr = sqlstr + table + " ( " + strings.Join(columns, ",") + " );"
	debug.Printf(sqlstr)
	_, err := db.Exec(sqlstr)
	return err
}

func (db DDB) Select(writer *csv.Writer, sqlstr string) error {
	sqlstr = strings.TrimSpace(sqlstr)
	if sqlstr == "" {
		return errors.New("ERROR: no SQL statement")
	}
	debug.Printf(sqlstr)
	rows, err := db.Query(sqlstr)
	if err != nil {
		return fmt.Errorf("ERROR: %s [%s]", err, sqlstr)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ERROR: Rows %s", err)
	}
	values := make([]sql.RawBytes, len(columns))
	results := make([]string, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("ERROR: %s", err)
		}
		for i, col := range values {
			results[i] = string(col)
		}
		writer.Write(results)
	}
	writer.Flush()
	return nil
}

func (db DDB) escapetable(oldname string) (newname string) {
	if db.dbdriver == "postgres" {
		if oldname[0] != '"' {
			newname = "\"" + oldname + "\""
		} else {
			newname = oldname
		}
	} else {
		if oldname[0] != '`' {
			newname = "`" + oldname + "`"
		} else {
			newname = oldname
		}
	}
	return newname
}

func rewrite(sqlstr string, oldname string, newname string) (rewrite string) {
	rewrite = strings.Replace(sqlstr, oldname, newname, -1)
	return rewrite
}

func sqlparse(sqlstr string) []string {
	var tablenames []string
	word := strings.Fields(sqlstr)
	for i := 0; i < len(word); i++ {
		if element := strings.ToUpper(word[i]); element == "FROM" || element == "JOIN" {
			if (i + 1) < len(word) {
				tablenames = append(tablenames, word[i+1])
			}
		}
	}
	return tablenames
}
