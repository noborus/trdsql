package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

// DDB is *sql.DB wrapper.
type DDB struct {
	driver    string
	dsn       string
	escape    string
	rewritten []string
	maxBulk   int
	*sql.DB
	tx *sql.Tx
}

// iTable is import Table data
type iTable struct {
	tablename string
	header    []string
	columns   []string
	place     string
	firstrow  bool
	sqlpre    string
}

// ImportData is import to the table.
func (db *DDB) ImportData(tablename string, header []string, input Input, firstrow bool) error {
	var err error
	columns := make([]string, len(header))
	for i := range header {
		columns[i] = db.escape + header[i] + db.escape
	}
	itable := &iTable{
		tablename: tablename,
		header:    header,
		columns:   columns,
		firstrow:  firstrow,
	}
	if db.driver == "postgres" {
		err = db.copyImport(itable, input)
	} else {
		err = db.insertImport(itable, input)
	}
	return err
}

func (db *DDB) copyImport(itable *iTable, input Input) error {
	sqlstr := fmt.Sprintf("COPY %s (%s) FROM STDIN", itable.tablename, strings.Join(itable.columns, ","))
	debug.Printf(sqlstr)
	stmt, err := db.tx.Prepare(sqlstr)
	if err != nil {
		return fmt.Errorf("COPY Prepare: %s", err)
	}
	row := make([]interface{}, len(itable.header))
	if itable.firstrow {
		row = input.firstRow(row)
		_, err = stmt.Exec(row...)
		if err != nil {
			return err
		}
	}

	for {
		row, err = input.rowRead(row)
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("Read: %s", err)
		}
		_, err = stmt.Exec(row...)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = stmt.Close()
	return err
}

func (db *DDB) insertImport(itable *iTable, input Input) error {
	var stmt *sql.Stmt
	var err error
	defer db.stmtClose(stmt)
	itable.sqlpre = fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		itable.tablename, strings.Join(itable.columns, ","))
	itable.place = "(" + strings.Repeat("?,", len(itable.header)-1) + "?)"

	row := make([]interface{}, len(itable.header))
	maxBulk := (db.maxBulk / len(row)) * len(row)
	bulk := make([]interface{}, 0, maxBulk)
	count := 0
	bulkNum := 0

	if itable.firstrow {
		row = input.firstRow(row)
		bulk = append(bulk, row...)
		bulkNum = bulkNum + len(row)
		count++
	}

	previousNum := 0
	eof := false
	for !eof {
		for bulkNum < maxBulk {
			row, err = input.rowRead(row)
			if err == nil {
				bulk = append(bulk, row...)
				bulkNum = bulkNum + len(row)
				count++
			} else if err == io.EOF {
				if len(bulk) <= 0 {
					return nil
				}
				eof = true
				break
			} else {
				return fmt.Errorf("Read: %s", err)
			}
		}

		if previousNum != bulkNum {
			previousNum = bulkNum
			if stmt != nil {
				err = stmt.Close()
				if err != nil {
					return err
				}
			}
			stmt, err = db.insertPrepare(itable, count)
			if err != nil {
				return err
			}
		}
		_, err = stmt.Exec(bulk...)
		if err != nil {
			return err
		}
		bulk = bulk[:0]
		bulkNum = 0
		count = 0
	}

	return err
}

func (db *DDB) stmtClose(stmt *sql.Stmt) {
	if stmt != nil {
		stmt.Close()
	}
}

func (db *DDB) insertPrepare(itable *iTable, count int) (*sql.Stmt, error) {
	sqlstr := itable.sqlpre +
		strings.Repeat(itable.place+",", count-1) + itable.place
	debug.Printf(sqlstr)
	stmt, err := db.tx.Prepare(sqlstr)
	if err != nil {
		return nil, fmt.Errorf("INSERT Prepare: %s:%s", sqlstr, err)
	}
	return stmt, nil
}

// Connect is connects to the database
func Connect(driver, dsn string) (*DDB, error) {
	var db DDB
	var err error
	db.driver = driver
	db.dsn = dsn
	switch driver {
	case "sqlite3":
		db.maxBulk = 500
		db.escape = "`"
		if dsn == "" {
			db.dsn = ":memory:"
		}
	case "mysql":
		db.escape = "`"
		db.maxBulk = 1000
	case "postgres":
		db.escape = "\""
	}
	db.DB, err = sql.Open(db.driver, db.dsn)
	return &db, err
}

// Disconnect is disconnect the database
func (db *DDB) Disconnect() error {
	err := db.Close()
	return err
}

// Create is create a temporary table
func (db *DDB) Create(table string, header []string) error {
	var sqlstr string
	columns := make([]string, len(header))
	for i := 0; i < len(header); i++ {
		columns[i] = db.escape + header[i] + db.escape + " text"
	}
	sqlstr = "CREATE TEMPORARY TABLE "
	sqlstr = sqlstr + table + " ( " + strings.Join(columns, ",") + " );"
	debug.Printf(sqlstr)
	_, err := db.tx.Exec(sqlstr)
	return err
}

// Select is executes SQL select statements
func (db *DDB) Select(sqlstr string) (*sql.Rows, error) {
	sqlstr = strings.TrimSpace(sqlstr)
	if sqlstr == "" {
		return nil, errors.New("no SQL statement")
	}
	debug.Printf(sqlstr)
	rows, err := db.tx.Query(sqlstr)
	if err != nil {
		return rows, fmt.Errorf("SQL:%s\n[%s]", err, sqlstr)
	}
	return rows, nil
}

func (db *DDB) escapetable(oldname string) string {
	var newname string
	if oldname[0] != db.escape[0] {
		newname = db.escape + oldname + db.escape
	} else {
		newname = oldname
	}
	return newname
}

func (db *DDB) rewrite(sqlstr string, oldname string, newname string) (rewrite string) {
	for _, rewritten := range db.rewritten {
		if rewritten == newname {
			// Rewritten
			return sqlstr
		}
	}
	rewrite = strings.Replace(sqlstr, oldname, newname, -1)
	db.rewritten = append(db.rewritten, newname)
	return rewrite
}

func sqlparse(sqlstr string) []string {
	var tablenames []string
	word := strings.Fields(sqlstr)
	for i, w := range word {
		if element := strings.ToUpper(w); element == "FROM" || element == "JOIN" {
			if (i + 1) < len(word) {
				tablenames = append(tablenames, word[i+1])
			}
		}
	}
	return tablenames
}
