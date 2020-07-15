package trdsql

import (
	"context"
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

var (
	// ErrNoTransaction is returned if SQL is executed when a transaction has not started.
	// SQL must be executed within a transaction.
	ErrNoTransaction = errors.New("transaction has not been started")
	// ErrNilReader is returned by Set reader of the specified file is nil error.
	ErrNilReader = errors.New("nil reader")
	// ErrInvalidNames is returned by Set if invalid names(number of columns is 0).
	ErrInvalidNames = errors.New("invalid names")
	// ErrInvalidTypes is returned by Set if invalid column types (does not match the number of column names).
	ErrInvalidTypes = errors.New("invalid types")
	// ErrNoStatement is returned by no SQL statement.
	ErrNoStatement = errors.New("no SQL statement")
)

// DB represents database information.
type DB struct {
	// driver holds the sql driver as a string.
	driver string
	// dsn holds dsn of sql as a character string.
	dsn string
	// quote is the quote character(s) that varies depending on the sql driver.
	// PostgreSQL is ("), sqlite3 and mysql is (`).
	quote string
	// maxBulk is the maximum number of bundles for bulk insert.
	// The number of columns x rows is less than maxBulk.
	maxBulk int
	// *sql.DB represents the database connection.
	*sql.DB
	// Tx represents a database transaction.
	Tx *sql.Tx
}

// Connect is connects to the database.
// Currently supported drivers are sqlite3, mysql, postgres.
// Set quote character and maxBulk depending on the driver type.
func Connect(driver, dsn string) (*DB, error) {
	var err error

	db := &DB{}
	db.DB, err = sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	db.driver = driver
	db.dsn = dsn
	debug.Printf("driver: %s, dsn: %s", driver, dsn)

	switch driver {
	case "sqlite3":
		db.quote = "`"
		db.maxBulk = 500
	case "mysql":
		db.quote = "`"
		db.maxBulk = 1000
	case "postgres":
		db.quote = "\""
	}

	return db, nil
}

// Disconnect is disconnect the database.
func (db *DB) Disconnect() error {
	return db.Close()
}

// CreateTable is create a (temporary) table in the database.
// The arguments are the table name, column name, column type, and temporary flag.
func (db *DB) CreateTable(tableName string, columnNames []string, columnTypes []string, isTemporary bool) error {
	return db.CreateTableContext(context.Background(), tableName, columnNames, columnTypes, isTemporary)
}

// CreateTableContext is create a (temporary) table in the database.
// The arguments are the table name, column name, column type, and temporary flag.
func (db *DB) CreateTableContext(ctx context.Context, tableName string, columnNames []string, columnTypes []string, isTemporary bool) error {
	if len(columnNames) == 0 {
		return ErrInvalidNames
	}
	if len(columnNames) != len(columnTypes) {
		return ErrInvalidTypes
	}
	if db.Tx == nil {
		return ErrNoTransaction
	}

	query := "CREATE TABLE "
	if isTemporary {
		query = "CREATE TEMPORARY TABLE "
	}

	columns := make([]string, len(columnNames))
	for i := 0; i < len(columnNames); i++ {
		columns[i] = db.QuotedName(columnNames[i]) + " " + columnTypes[i]
	}
	query += tableName + " ( " + strings.Join(columns, ",") + " );"
	debug.Printf(query)
	_, err := db.Tx.ExecContext(ctx, query)
	return err
}

// importTable represents the table data to be imported.
type importTable struct {
	tableName string
	columns   []string
	query     string
	place     string
	maxCap    int
	row       []interface{}
	lastCount int
	count     int
}

// Import is imports data into a table.
func (db *DB) Import(tableName string, columnNames []string, reader Reader) error {
	return db.ImportContext(context.Background(), tableName, columnNames, reader)
}

// ImportContext is imports data into a table.
func (db *DB) ImportContext(ctx context.Context, tableName string, columnNames []string, reader Reader) error {
	if db.Tx == nil {
		return ErrNoTransaction
	}
	if reader == nil {
		return ErrNilReader
	}

	columns := make([]string, len(columnNames))
	for i := range columnNames {
		columns[i] = db.QuotedName(columnNames[i])
	}

	row := make([]interface{}, len(columnNames))
	table := &importTable{
		tableName: tableName,
		columns:   columns,
		row:       row,
		lastCount: 0,
		count:     0,
	}

	if db.driver == "postgres" {
		return db.copyImport(ctx, table, reader)
	}
	return db.insertImport(ctx, table, reader)
}

// copyImport adds rows to a table with the COPY clause (PostgreSQL only).
func (db *DB) copyImport(ctx context.Context, table *importTable, reader Reader) error {
	query := "COPY " + table.tableName + " (" + strings.Join(table.columns, ",") + ") FROM STDIN"
	debug.Printf(query)

	stmt, err := db.Tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("COPY prepare: %w", err)
	}

	preReadRows := reader.PreReadRow()
	for _, row := range preReadRows {
		if row == nil {
			break
		}
		_, err = stmt.ExecContext(ctx, row...)
		if err != nil {
			return err
		}
	}

	for {
		table.row, err = reader.ReadRow(table.row)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("COPY read: %w", err)
		}
		// Skip when empty read.
		if len(table.row) == 0 {
			continue
		}
		_, err = stmt.ExecContext(ctx, table.row...)
		if err != nil {
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	db.stmtClose(stmt)

	return nil
}

// insertImport adds a row to a table with an INSERT clause.
// Insert multiple rows by bulk insert.
func (db *DB) insertImport(ctx context.Context, table *importTable, reader Reader) error {
	var err error
	var stmt *sql.Stmt

	// #nosec G202
	table.query = "INSERT INTO " + table.tableName + " (" + strings.Join(table.columns, ",") + ") VALUES "
	table.place = "(" + strings.Repeat("?,", len(table.columns)-1) + "?)"
	table.maxCap = (db.maxBulk / len(table.row)) * len(table.row)
	bulk := make([]interface{}, 0, table.maxCap)

	preRows := reader.PreReadRow()
	preRowNum := len(preRows)
	preCount := 0
	for eof := false; !eof; {
		if preCount < preRowNum {
			// PreRead
			for preCount < preRowNum {
				row := preRows[preCount]
				bulk = append(bulk, row...)
				table.count++
				preCount++
				if (table.count * len(table.row)) > table.maxCap {
					break
				}
			}
		} else {
			// Read
			bulk, err = bulkPush(table, reader, bulk)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					return fmt.Errorf("bulk read: %w", err)
				}
				eof = true
				if len(bulk) == 0 {
					return nil
				}
			}
		}

		stmt, err = db.bulkStmtOpen(table, stmt)
		if err != nil {
			return err
		}
		_, err = stmt.ExecContext(ctx, bulk...)
		if err != nil {
			return err
		}
		bulk = bulk[:0]
		table.count = 0
	}
	db.stmtClose(stmt)
	return nil
}

func (db *DB) stmtClose(stmt *sql.Stmt) {
	if stmt == nil {
		return
	}
	if err := stmt.Close(); err != nil {
		log.Printf("ERROR: stmtClose:%s", err)
	}
}

func bulkPush(table *importTable, input Reader, bulk []interface{}) ([]interface{}, error) {
	var err error
	for (table.count * len(table.row)) < table.maxCap {
		table.row, err = input.ReadRow(table.row)
		if err != nil {
			return bulk, err
		}
		// Skip when empty read.
		if len(table.row) == 0 {
			continue
		}

		bulk = append(bulk, table.row...)
		table.count++
	}
	return bulk, nil
}

func (db *DB) bulkStmtOpen(table *importTable, stmt *sql.Stmt) (*sql.Stmt, error) {
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

func (db *DB) insertPrepare(table *importTable) (*sql.Stmt, error) {
	query := table.query +
		strings.Repeat(table.place+",", table.count-1) + table.place
	debug.Printf(query)
	stmt, err := db.Tx.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("INSERT Prepare: %s:%w", query, err)
	}
	return stmt, nil
}

// QuotedName returns the table name quoted.
// Returns as is, if already quoted.
func (db *DB) QuotedName(oldName string) string {
	if oldName[0] != db.quote[0] {
		return db.quote + oldName + db.quote
	}
	return oldName
}

// Select is executes SQL select statements.
func (db *DB) Select(query string) (*sql.Rows, error) {
	return db.SelectContext(context.Background(), query)
}

// SelectContext is executes SQL select statements with context.
func (db *DB) SelectContext(ctx context.Context, query string) (*sql.Rows, error) {
	if db.Tx == nil {
		return nil, ErrNoTransaction
	}

	query = strings.TrimSpace(query)
	if query == "" {
		return nil, ErrNoStatement
	}
	debug.Printf(query)

	rows, err := db.Tx.QueryContext(ctx, query)
	if err != nil {
		return rows, fmt.Errorf("%w [%s]", err, query)
	}
	return rows, nil
}
