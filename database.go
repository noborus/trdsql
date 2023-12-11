package trdsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
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
	// importCount represents the number that is incremented to identify imported files.
	importCount int
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
	if db.Tx == nil {
		return ErrNoTransaction
	}
	if len(columnNames) == 0 {
		return ErrInvalidNames
	}
	if len(columnNames) != len(columnTypes) {
		return ErrInvalidTypes
	}

	query := db.queryCreateTable(tableName, columnNames, columnTypes, isTemporary)
	debug.Printf(query)
	_, err := db.Tx.ExecContext(ctx, query)
	return err
}

func (db *DB) queryCreateTable(tableName string, columnNames []string, columnTypes []string, isTemporary bool) string {
	var buf strings.Builder
	if isTemporary {
		buf.WriteString("CREATE TEMPORARY TABLE ")
	} else {
		buf.WriteString("CREATE TABLE ")
	}
	buf.WriteString(tableName)
	buf.WriteString(" ( ")
	buf.WriteString(db.QuotedName(columnNames[0]))
	buf.WriteString(" ")
	buf.WriteString(columnTypes[0])
	for i, columnName := range columnNames[1:] {
		buf.WriteString(", ")
		buf.WriteString(db.QuotedName(columnName))
		buf.WriteString(" ")
		buf.WriteString(columnTypes[i+1])
	}
	buf.WriteString(" );")
	return buf.String()
}

// importTable represents the table data to be imported.
type importTable struct {
	tableName string
	columns   []string
	row       []interface{}
	maxCap    int
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
	query := queryCopy(table)
	debug.Printf(query)

	stmt, err := db.Tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("COPY prepare: %w", err)
	}
	defer db.stmtClose(stmt)

	preReadRows := reader.PreReadRow()
	for _, row := range preReadRows {
		if row == nil {
			break
		}
		if _, err = stmt.ExecContext(ctx, row...); err != nil {
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
		if _, err = stmt.ExecContext(ctx, table.row...); err != nil {
			return err
		}
	}

	_, err = stmt.ExecContext(ctx)
	return err
}

// queryCopy constructs a SQL COPY statement.
func queryCopy(table *importTable) string {
	var buf strings.Builder
	buf.WriteString("COPY ")
	buf.WriteString(table.tableName)
	buf.WriteString(" (")
	buf.WriteString(table.columns[0])
	for _, column := range table.columns[1:] {
		buf.WriteString(", ")
		buf.WriteString(column)
	}
	buf.WriteString(") FROM STDIN;")
	return buf.String()
}

// insertImport adds a row to a table with an INSERT clause.
// Insert multiple rows by bulk insert.
func (db *DB) insertImport(ctx context.Context, table *importTable, reader Reader) error {
	var err error
	var stmt *sql.Stmt
	defer db.stmtClose(stmt)

	if len(table.row) > db.maxBulk {
		table.maxCap = len(table.row)
	} else {
		table.maxCap = (db.maxBulk / len(table.row)) * len(table.row)
	}
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
			bulk, err = bulkPush(ctx, table, reader, bulk)
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

		stmt, err = db.bulkStmtOpen(ctx, table, stmt)
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, bulk...); err != nil {
			return err
		}
		bulk = bulk[:0]
		table.count = 0
	}
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

func bulkPush(ctx context.Context, table *importTable, input Reader, bulk []interface{}) ([]interface{}, error) {
	for (table.count * len(table.row)) < table.maxCap {
		row, err := input.ReadRow(table.row)
		if err != nil {
			return bulk, err
		}
		// Skip when empty read.
		if len(row) == 0 {
			continue
		}

		bulk = append(bulk, row...)
		table.count++
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
	return bulk, nil
}

func (db *DB) bulkStmtOpen(ctx context.Context, table *importTable, stmt *sql.Stmt) (*sql.Stmt, error) {
	if table.lastCount == table.count {
		return stmt, nil
	}
	db.stmtClose(stmt)
	stmt, err := db.insertPrepare(ctx, table)
	if err != nil {
		return nil, err
	}
	table.lastCount = table.count
	return stmt, nil
}

func (db *DB) insertPrepare(ctx context.Context, table *importTable) (*sql.Stmt, error) {
	query := queryInsert(table)
	debug.Printf(query)

	stmt, err := db.Tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("INSERT Prepare: %s:%w", query, err)
	}
	return stmt, nil
}

// queryInsert constructs a SQL INSERT statement.
func queryInsert(table *importTable) string {
	var buf strings.Builder
	buf.WriteString("INSERT INTO ")
	buf.WriteString(table.tableName)
	buf.WriteString(" (")
	buf.WriteString(table.columns[0])
	for _, column := range table.columns[1:] {
		buf.WriteString(", ")
		buf.WriteString(column)
	}
	buf.WriteString(") VALUES ")
	buf.WriteString("(")
	buf.WriteString("?")
	for i := 1; i < len(table.columns); i++ {
		buf.WriteString(",?")
	}
	buf.WriteString(")")
	for i := 1; i < table.count; i++ {
		buf.WriteString(",(")
		buf.WriteString("?")
		for j := 1; j < len(table.columns); j++ {
			buf.WriteString(",?")
		}
		buf.WriteString(")")
	}
	buf.WriteString(";")
	return buf.String()
}

// QuotedName returns the table name quoted.
// Returns as is, if already quoted.
func (db *DB) QuotedName(orgName string) string {
	if orgName == "" {
		return ""
	}
	if orgName[0] == db.quote[0] {
		return orgName
	}

	var buf strings.Builder
	buf.WriteString(db.quote)
	buf.WriteString(orgName)
	buf.WriteString(db.quote)
	return buf.String()
}

// Select is executes SQL select statements.
func (db *DB) Select(query string) (*sql.Rows, error) {
	return db.SelectContext(context.Background(), query)
}

// SelectContext is executes SQL select statements with context.
// SelectContext is a wrapper for QueryContext.
func (db *DB) SelectContext(ctx context.Context, query string) (*sql.Rows, error) {
	rows, err := db.Tx.QueryContext(ctx, query)
	if err != nil {
		return rows, fmt.Errorf("%w [%s]", err, query)
	}
	return rows, nil
}

func (db *DB) OtherExecContext(ctx context.Context, query string) error {
	_, err := db.Tx.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%w [%s]", err, query)
	}
	return nil
}
