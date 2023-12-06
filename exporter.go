package trdsql

import (
	"context"
	"log"

	"github.com/noborus/sqlss"
)

// Exporter is the interface for processing query results.
// Exporter executes SQL and outputs to Writer.
type Exporter interface {
	Export(db *DB, sql string) error
	ExportContext(ctx context.Context, db *DB, sql string) error
}

// WriteFormat represents a structure that satisfies Exporter.
type WriteFormat struct {
	Writer
}

// NewExporter returns trdsql default Exporter.
func NewExporter(writer Writer) *WriteFormat {
	return &WriteFormat{
		Writer: writer,
	}
}

// Export is execute SQL(Select) and the result is written out by the writer.
// Export is called from Exec.
func (e *WriteFormat) Export(db *DB, sql string) error {
	ctx := context.Background()
	return e.ExportContext(ctx, db, sql)
}

// ExportContext is execute SQL(Select) and the result is written out by the writer.
// ExportContext is called from ExecContext.
func (e *WriteFormat) ExportContext(ctx context.Context, db *DB, sql string) error {
	queries := sqlss.SplitQueries(sql)
	if !multi || len(queries) == 1 {
		return e.exportContext(ctx, db, false, sql)
	}
	for _, query := range queries {
		if err := e.exportContext(ctx, db, true, query); err != nil {
			return err
		}
	}
	return nil
}

func (e *WriteFormat) exportContext(ctx context.Context, db *DB, multi bool, query string) error {
	rows, err := db.SelectContext(ctx, query)
	if err != nil {
		return err
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	defer func() {
		if err = rows.Close(); err != nil {
			log.Printf("ERROR: close:%s", err)
		}
	}()

	// No data is not output for multiple queries.
	if multi && len(columns) == 0 {
		return nil
	}
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	types := make([]string, len(columns))
	for i, ct := range columnTypes {
		types[i] = ct.DatabaseTypeName()
	}

	if err := e.Writer.PreWrite(columns, types); err != nil {
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
		if err := e.Writer.WriteRow(values, columns); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return e.Writer.PostWrite()
}
