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
	escape   string
	*sql.DB
	stmt *sql.Stmt
}

func rowImport(stmt *sql.Stmt, list []interface{}) {
	_, err := stmt.Exec(list...)
	if err != nil {
		log.Println(err)
	}
}

func (db DDB) ImportPrepare(table string, header []string, head bool) (DDB, error) {
	columns := make([]string, len(header))
	place := make([]string, len(header))
	for i := range header {
		if head {
			columns[i] = db.escape + header[i] + db.escape
		} else {
			columns[i] = "c" + strconv.Itoa(i+1)
		}
		if db.dbdriver == "postgres" {
			place[i] = "$" + strconv.Itoa(i+1)
		} else {
			place[i] = "?"
		}
	}
	sqlstr := "INSERT INTO " + table + " (" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(place, ",") + ");"
	debug.Printf(sqlstr)
	var err error
	db.stmt, err = db.Prepare(sqlstr)
	if err != nil {
		return db, fmt.Errorf("ERROR INSERT Prepare: %s", err)
	}
	return db, nil
}

func (db DDB) Import(reader *csv.Reader, header []string, head bool) error {
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
		for i := range header {
			list[i] = record[i]
		}
		rowImport(db.stmt, list)
	}
	return nil
}

func Connect(driver, dsn string) (DDB, error) {
	var db DDB
	var err error
	db.dbdriver = driver
	db.dbdsn = dsn
	if driver == "postgres" {
		db.escape = "\""
	} else {
		db.escape = "`"
	}
	db.DB, err = sql.Open(driver, dsn)
	return db, err
}

func (db DDB) Disconnect() error {
	err := db.Close()
	return err
}

func (db DDB) Create(table string, header []string, head bool) error {
	var sqlstr string
	columns := make([]string, len(header))
	for i := 0; i < len(header); i++ {
		if head {
			columns[i] = db.escape + header[i] + db.escape + " text"
		} else {
			columns[i] = "c" + strconv.Itoa(i+1) + " text"
		}
	}
	sqlstr = "CREATE TEMPORARY TABLE "
	sqlstr = sqlstr + table + " ( " + strings.Join(columns, ",") + " );"
	debug.Printf(sqlstr)
	_, err := db.Exec(sqlstr)
	return err
}

func (db DDB) Select(writer *csv.Writer, sqlstr string, head bool) error {
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
	if head {
		writer.Write(columns)
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

func (db DDB) escapetable(oldname string) string {
	var newname string
	if oldname[0] != db.escape[0] {
		newname = db.escape + oldname + db.escape
	} else {
		newname = oldname
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
