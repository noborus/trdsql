package trdsql

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	// MySQL driver
	_ "github.com/go-sql-driver/mysql"
	// PostgreSQL driver
	_ "github.com/lib/pq"
	// SQLite3 driver
	_ "github.com/mattn/go-sqlite3"
)

// DB is *sql.DB wrapper.
type DB struct {
	driver    string
	dsn       string
	escape    string
	rewritten []string
	maxBulk   int
	*sql.DB
	Tx *sql.Tx
}

// Connect is connects to the database
func Connect(driver, dsn string) (*DB, error) {
	var db DB
	var err error
	db.DB, err = sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
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
	debug.Printf("driver: %s, dsn: %s", driver, dsn)
	return &db, nil
}

// Disconnect is disconnect the database
func (db *DB) Disconnect() error {
	err := db.Close()
	return err
}

// CreateTable is create a temporary table
func (db *DB) CreateTable(tableName string, names []string, types []string, isTemporary bool) error {
	if len(names) == 0 {
		return errors.New("missing names")
	}
	if len(names) != len(types) {
		return errors.New("missing types")
	}
	if db.Tx == nil {
		return errors.New("transaction has not been started")
	}
	columns := make([]string, len(names))
	for i := 0; i < len(names); i++ {
		columns[i] = db.escape + names[i] + db.escape + " " + types[i]
	}
	query := "CREATE "
	if isTemporary {
		query += "TEMPORARY TABLE "
	} else {
		query += "TABLE "
	}
	query += tableName + " ( " + strings.Join(columns, ",") + " );"
	debug.Printf(query)
	_, err := db.Tx.Exec(query)
	return err
}

// Select is executes SQL select statements
func (db *DB) Select(query string) (*sql.Rows, error) {
	if db.Tx == nil {
		return nil, errors.New("transaction has not been started")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, errors.New("no SQL statement")
	}
	debug.Printf(query)
	rows, err := db.Tx.Query(query)
	if err != nil {
		return rows, fmt.Errorf("SQL:%s\n[%s]", err, query)
	}
	return rows, nil
}

// Table is import Table data
type Table struct {
	tableName string
	columns   []string
	query     string
	place     string
	maxCap    int
	row       []interface{}
	lastCount int
	count     int
}

// Import is import to the table.
func (db *DB) Import(tableName string, columnNames []string, reader Reader) error {
	if db.Tx == nil {
		return errors.New("transaction has not been started")
	}
	if reader == nil {
		return errors.New("nil reader")
	}
	var err error
	columns := make([]string, len(columnNames))
	for i := range columnNames {
		columns[i] = db.escape + columnNames[i] + db.escape
	}
	row := make([]interface{}, len(columnNames))
	table := &Table{
		tableName: tableName,
		columns:   columns,
		row:       row,
		lastCount: 0,
		count:     0,
	}
	if db.driver == "postgres" {
		err = db.copyImport(table, reader)
	} else {
		err = db.insertImport(table, reader)
	}
	return err
}

func (db *DB) copyImport(table *Table, reader Reader) error {
	query := "COPY " + table.tableName + " (" + strings.Join(table.columns, ",") + ") FROM STDIN"
	debug.Printf(query)
	stmt, err := db.Tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("COPY Prepare: %s", err)
	}
	preReadRows := reader.PreReadRow()
	for _, row := range preReadRows {
		if row == nil {
			break
		}
		_, err = stmt.Exec(row...)
		if err != nil {
			return err
		}
	}

	for {
		table.row, err = reader.ReadRow(table.row)
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("read: %s", err)
		}
		_, err = stmt.Exec(table.row...)
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

func (db *DB) insertImport(table *Table, reader Reader) error {
	var err error
	var stmt *sql.Stmt
	defer db.stmtClose(stmt)
	// #nosec G202
	table.query = "INSERT INTO " + table.tableName + " (" + strings.Join(table.columns, ",") + ") VALUES "
	table.place = "(" + strings.Repeat("?,", len(table.columns)-1) + "?)"
	table.maxCap = (db.maxBulk / len(table.row)) * len(table.row)
	bulk := make([]interface{}, 0, table.maxCap)

	pRows := reader.PreReadRow()
	for eof := false; !eof; {
		if len(pRows) > 0 {
			for (table.count * len(table.row)) < table.maxCap {
				if len(pRows) == 0 {
					break
				}
				row := pRows[len(pRows)-1]
				pRows = pRows[:len(pRows)-1]
				bulk = append(bulk, row...)
				table.count++
			}
		} else {
			bulk, err = bulkPush(table, reader, bulk)
			if err == io.EOF {
				if len(bulk) == 0 {
					return nil
				}
				eof = true
			} else if err != nil {
				return fmt.Errorf("read: %s", err)
			}
		}
		stmt, err = db.bulkStmtOpen(table, stmt)
		if err != nil {
			return err
		}
		_, err = stmt.Exec(bulk...)
		if err != nil {
			return err
		}
		bulk = bulk[:0]
		table.count = 0
	}
	return nil
}

func bulkPush(table *Table, input Reader, bulk []interface{}) ([]interface{}, error) {
	var err error
	for (table.count * len(table.row)) < table.maxCap {
		table.row, err = input.ReadRow(table.row)
		if err != nil {
			return bulk, err
		}
		bulk = append(bulk, table.row...)
		table.count++
	}
	return bulk, nil
}

func (db *DB) bulkStmtOpen(table *Table, stmt *sql.Stmt) (*sql.Stmt, error) {
	var err error

	if table.lastCount != table.count {
		if stmt != nil {
			err = stmt.Close()
			if err != nil {
				return nil, err
			}
		}
		stmt, err = db.insertPrepare(table)
		if err != nil {
			return nil, err
		}
		table.lastCount = table.count
	}
	return stmt, nil
}

func (db *DB) stmtClose(stmt *sql.Stmt) {
	if stmt != nil {
		err := stmt.Close()
		if err != nil {
			log.Printf("ERROR: stmtClose:%s", err)
		}
	}
}

func (db *DB) insertPrepare(table *Table) (*sql.Stmt, error) {
	query := table.query +
		strings.Repeat(table.place+",", table.count-1) + table.place
	debug.Printf(query)
	stmt, err := db.Tx.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("INSERT Prepare: %s:%s", query, err)
	}
	return stmt, nil
}

// EscapeName is escape table name.
func (db *DB) EscapeName(oldName string) string {
	var newName string
	if oldName[0] != db.escape[0] {
		newName = db.escape + oldName + db.escape
	} else {
		newName = oldName
	}
	return newName
}

// RewriteSQL is rewrite SQL from file name to table name.
func (db *DB) RewriteSQL(query string, oldName string, newName string) (rewrite string) {
	for _, rewritten := range db.rewritten {
		if rewritten == newName {
			// Rewritten
			return query
		}
	}
	rewrite = strings.Replace(query, oldName, newName, -1)
	db.rewritten = append(db.rewritten, newName)
	return rewrite
}
