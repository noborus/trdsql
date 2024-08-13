package trdsql

import (
	"context"
	"database/sql"
	"log"
	"strings"

	"github.com/noborus/sqlss"
)

// Exporter is the interface for processing query results.
// Exporter executes SQL and outputs to Writer.
type Exporter interface {
	Export(ctx context.Context, db *DB, sql string) error
}

// WriteFormat represents a structure that satisfies Exporter.
type WriteFormat struct {
	Writer
	columns []string
	types   []string
	multi   bool
}

// NewExporter returns trdsql default Exporter.
func NewExporter(writer Writer) *WriteFormat {
	return &WriteFormat{
		Writer: writer,
		multi:  false,
	}
}

// Export is execute SQL(Select) and the result is written out by the writer.
// Export is called from ExecContext.
func (e *WriteFormat) Export(ctx context.Context, db *DB, sqlQuery string) error {
	queries := sqlss.SplitQueries(sqlQuery)
	if !multi || len(queries) == 1 {
		return e.export(ctx, db, sqlQuery)
	}

	e.multi = true
	for _, query := range queries {
		if err := e.export(ctx, db, query); err != nil {
			return err
		}
	}
	return nil
}

func (e *WriteFormat) export(ctx context.Context, db *DB, query string) error {
	if db.Tx == nil {
		return ErrNoTransaction
	}

	query = strings.TrimSpace(query)
	if query == "" {
		return ErrNoStatement
	}
	debug.Printf(query)

	if db.isExec(query) {
		return db.OtherExec(ctx, query)
	}

	rows, err := db.Select(ctx, query)
	if err != nil {
		return err
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	e.columns = columns

	defer func() {
		if err = rows.Close(); err != nil {
			log.Printf("ERROR: close:%s", err)
		}
	}()

	// No data is not output for multiple queries.
	if e.multi && len(e.columns) == 0 {
		return nil
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	types := make([]string, len(columns))
	for i, ct := range columnTypes {
		types[i] = ct.DatabaseTypeName()
	}
	e.types = types

	return e.write(ctx, rows)
}

func (e *WriteFormat) write(ctx context.Context, rows *sql.Rows) error {
	values := make([]any, len(e.columns))
	scanArgs := make([]any, len(e.columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	if err := e.Writer.PreWrite(e.columns, e.types); err != nil {
		return err
	}

	for rows.Next() {
		select {
		case <-ctx.Done(): // cancellation
			return ctx.Err()
		default:
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		if err := e.Writer.WriteRow(values, e.columns); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return e.Writer.PostWrite()
}

// isExec returns true if the query is not a SELECT statement.
// Queries that return no rows in SQlite should use ExecContext and therefore return true.
func (db *DB) isExec(query string) bool {
	if db.driver == "sqlite3" || db.driver == "sqlite" {
		return !strings.HasPrefix(strings.ToUpper(query), "SELECT")
	}
	return false
}
