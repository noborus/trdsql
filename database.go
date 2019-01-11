package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
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

// Connect is connects to the database
func Connect(driver, dsn string) (*DDB, error) {
	var db DDB
	var err error
	db.driver = driver
	db.dsn = dsn
	switch driver {
	case "sqlite3":
		db.escape = "`"
		db.maxBulk = 500
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

// CreateTable is create a temporary table
func (db *DDB) CreateTable(table string, name []string) error {
	var sqlstr string
	columns := make([]string, len(name))
	for i := 0; i < len(name); i++ {
		columns[i] = db.escape + name[i] + db.escape + " text"
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

// iTable is import Table data
type iTable struct {
	tablename   string
	columnNames []string
	columns     []string
	sqlstr      string
	place       string
	maxCap      int
	preRead     int
	row         []interface{}
	lastCount   int
	count       int
}

// Import is import to the table.
func (db *DDB) Import(tablename string, columnNames []string, input Input, preRead int) error {
	var err error
	columns := make([]string, len(columnNames))
	for i := range columnNames {
		columns[i] = db.escape + columnNames[i] + db.escape
	}
	row := make([]interface{}, len(columnNames))
	itable := &iTable{
		tablename:   tablename,
		columnNames: columnNames,
		columns:     columns,
		preRead:     preRead,
		row:         row,
		lastCount:   0,
		count:       0,
	}
	if db.driver == "postgres" {
		err = db.copyImport(itable, input)
	} else {
		err = db.insertImport(itable, input)
	}
	return err
}

func (db *DDB) copyImport(itable *iTable, input Input) error {
	sqlstr := "COPY " + itable.tablename + " (" + strings.Join(itable.columns, ",") + ") FROM STDIN"
	debug.Printf(sqlstr)
	stmt, err := db.tx.Prepare(sqlstr)
	if err != nil {
		return fmt.Errorf("COPY Prepare: %s", err)
	}
	if itable.preRead > 0 {
		preReadRows := input.PreReadRow()
		for _, row := range preReadRows {
			if row == nil {
				break
			}
			_, err = stmt.Exec(row...)
			if err != nil {
				return err
			}
		}
	}

	for {
		itable.row, err = input.ReadRow(itable.row)
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("Read: %s", err)
		}
		_, err = stmt.Exec(itable.row...)
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
	var err error
	var stmt *sql.Stmt
	defer db.stmtClose(stmt)
	itable.sqlstr = "INSERT INTO " + itable.tablename + " (" + strings.Join(itable.columns, ",") + ") VALUES "
	itable.place = "(" + strings.Repeat("?,", len(itable.columnNames)-1) + "?)"
	itable.maxCap = (db.maxBulk / len(itable.row)) * len(itable.row)
	bulk := make([]interface{}, 0, itable.maxCap)

	var pRows [][]interface{}
	if itable.preRead > 0 {
		pRows = input.PreReadRow()
	}
	for eof := false; !eof; {
		if len(pRows) > 0 {
			for (itable.count * len(itable.row)) < itable.maxCap {
				if len(pRows) <= 0 {
					break
				}
				row := pRows[len(pRows)-1]
				pRows = pRows[:len(pRows)-1]
				bulk = append(bulk, row...)
				itable.count++
			}
		} else {
			bulk, err = bulkPush(itable, input, bulk)
			if err == io.EOF {
				if len(bulk) <= 0 {
					return nil
				}
				eof = true
			} else if err != nil {
				return fmt.Errorf("Read: %s", err)
			}
		}
		stmt, err = db.bulkStmtOpen(itable, stmt)
		if err != nil {
			return err
		}
		_, err = stmt.Exec(bulk...)
		if err != nil {
			return err
		}
		bulk = bulk[:0]
		itable.count = 0
	}
	return nil
}

func bulkPush(itable *iTable, input Input, bulk []interface{}) ([]interface{}, error) {
	var err error
	for (itable.count * len(itable.row)) < itable.maxCap {
		itable.row, err = input.ReadRow(itable.row)
		if err != nil {
			return bulk, err
		}
		bulk = append(bulk, itable.row...)
		itable.count++
	}
	return bulk, nil
}

func (db *DDB) bulkStmtOpen(itable *iTable, stmt *sql.Stmt) (*sql.Stmt, error) {
	var err error

	if itable.lastCount != itable.count {
		if stmt != nil {
			err = stmt.Close()
			if err != nil {
				return nil, err
			}
		}
		stmt, err = db.insertPrepare(itable)
		if err != nil {
			return nil, err
		}
		itable.lastCount = itable.count
	}
	return stmt, nil
}

func (db *DDB) stmtClose(stmt *sql.Stmt) {
	if stmt != nil {
		err := stmt.Close()
		if err != nil {
			log.Printf("ERROR: stmtClose:%s", err)
		}
	}
}

func (db *DDB) insertPrepare(itable *iTable) (*sql.Stmt, error) {
	sqlstr := itable.sqlstr +
		strings.Repeat(itable.place+",", itable.count-1) + itable.place
	debug.Printf(sqlstr)
	stmt, err := db.tx.Prepare(sqlstr)
	if err != nil {
		return nil, fmt.Errorf("INSERT Prepare: %s:%s", sqlstr, err)
	}
	return stmt, nil
}

// EscapeTable is escape table name.
func (db *DDB) EscapeTable(oldname string) string {
	var newname string
	if oldname[0] != db.escape[0] {
		newname = db.escape + oldname + db.escape
	} else {
		newname = oldname
	}
	return newname
}

// RewriteSQL is rewrite SQL from file name to table name.
func (db *DDB) RewriteSQL(sqlstr string, oldname string, newname string) (rewrite string) {
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
