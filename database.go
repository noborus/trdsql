package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
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
	*sql.DB
	stmt *sql.Stmt
}

func rowImport(stmt *sql.Stmt, list []interface{}) {
	_, err := stmt.Exec(list...)
	if err != nil {
		debug.Printf("%s\n", err)
	}
}

// InsertPrepare is executes SQL syntax INSERT with Prepare
func (db *DDB) InsertPrepare(table string, header []string) error {
	columns := make([]string, len(header))
	place := make([]string, len(header))
	for i := range header {
		columns[i] = db.escape + header[i] + db.escape
		if db.driver == "postgres" {
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
		return fmt.Errorf("ERROR INSERT Prepare: %s", err)
	}
	return nil
}

// Connect is connects to the database
func Connect(driver, dsn string) (*DDB, error) {
	var db DDB
	var err error
	if driver == "sqlite3" && dsn == "" {
		dsn = ":memory:"
	}
	db.driver = driver
	db.dsn = dsn
	if driver == "postgres" {
		db.escape = "\""
	} else {
		db.escape = "`"
	}
	db.DB, err = sql.Open(driver, dsn)
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
	_, err := db.Exec(sqlstr)
	return err
}

// Select is executes SQL select statements
func (db *DDB) Select(sqlstr string) (*sql.Rows, error) {
	sqlstr = strings.TrimSpace(sqlstr)
	if sqlstr == "" {
		return nil, errors.New("no SQL statement")
	}
	debug.Printf(sqlstr)
	rows, err := db.Query(sqlstr)
	if err != nil {
		return rows, fmt.Errorf("%s\n[%s]", err, sqlstr)
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
	for i := 0; i < len(word); i++ {
		if element := strings.ToUpper(word[i]); element == "FROM" || element == "JOIN" {
			if (i + 1) < len(word) {
				tablenames = append(tablenames, word[i+1])
			}
		}
	}
	return tablenames
}
