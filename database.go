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
	maxbulk   int
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
	count     int
	rows      []interface{}
	rownum    int
	sqlpre    string
	sqlstr    string
	stmt      *sql.Stmt
}

// ImportData is import to the table.
func (db *DDB) ImportData(tablename string, header []string, input Input, firstrow bool) error {
	var err error
	columns := make([]string, len(header))
	for i := range header {
		columns[i] = db.escape + header[i] + db.escape
	}
	rows := make([]interface{}, 0, db.maxbulk*len(header))
	itable := &iTable{
		tablename: tablename,
		header:    header,
		columns:   columns,
		firstrow:  firstrow,
		rows:      rows,
		rownum:    0,
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
	stmt.Exec()
	stmt.Close()
	return nil
}

func (db *DDB) insertImport(itable *iTable, input Input) error {
	var err error
	var stmt *sql.Stmt

	row := make([]interface{}, len(itable.header))

	itable.sqlpre = fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		itable.tablename, strings.Join(itable.columns, ","))

	if itable.firstrow {
		itable.place = "(" + strings.Repeat("?,", len(itable.header)-1) + "?)"
		sqlstr := itable.sqlpre + itable.place
		debug.Printf(sqlstr)

		stmt, err = db.tx.Prepare(sqlstr)
		if err != nil {
			return fmt.Errorf("INSERT Prepare: %s:%s", sqlstr, err)
		}
		row = input.firstRow(row)
		_, err = stmt.Exec(row...)
		if err != nil {
			return err
		}
		stmt.Exec()
		stmt.Close()
	}

	for {
		row, err = input.rowRead(row)
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			return fmt.Errorf("Read: %s", err)
		}
		err = db.bulkpush(itable, row)
		if err != nil {
			return err
		}
	}
	if itable.count > 0 {
		err = db.bulkimport(itable)
	}
	itable.stmt.Close()
	return err
}

func (db *DDB) bulkpush(itable *iTable, row []interface{}) error {
	var err error
	itable.count++
	for _, r := range row {
		itable.rows = append(itable.rows, r)
	}
	if (itable.count*len(row))+len(row) > db.maxbulk {
		err = db.bulkimport(itable)
		itable.rows = itable.rows[:0]
		itable.count = 0
	}
	return err
}

func (db *DDB) bulkimport(itable *iTable) error {
	var err error
	if itable.rownum != itable.count {
		if itable.stmt != nil {
			itable.stmt.Close()
		}
		itable.sqlstr = itable.sqlpre +
			strings.Repeat(itable.place+",", itable.count-1) + itable.place
		itable.rownum = itable.count
		debug.Printf(itable.sqlstr)
		itable.stmt, err = db.tx.Prepare(itable.sqlstr)
		if err != nil {
			return fmt.Errorf("INSERT Prepare: %s:%s", itable.sqlstr, err)
		}
	}
	_, err = itable.stmt.Exec(itable.rows...)
	return err
}

// Connect is connects to the database
func Connect(driver, dsn string) (*DDB, error) {
	var db DDB
	var err error
	db.driver = driver
	db.dsn = dsn
	switch driver {
	case "sqlite3":
		db.maxbulk = 1000
		db.escape = "`"
		if dsn == "" {
			db.dsn = ":memory:"
		}
	case "mysql":
		db.escape = "`"
		db.maxbulk = 1000
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
