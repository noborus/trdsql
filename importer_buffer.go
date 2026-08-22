package trdsql

import (
	"context"
	"io"
)

// BufferImporter includes tableName and Reader.
type BufferImporter struct {
	Reader
	tableName string
}

// NewBufferImporter returns a BufferImporter.
func NewBufferImporter(tableName string, r io.Reader, options ...ReadOpt) (*BufferImporter, error) {
	readOpts := NewReadOpts(options...)
	readOpts.realFormat = readOpts.InFormat
	reader, err := NewReader(r, readOpts)
	if err != nil {
		return nil, err
	}
	return &BufferImporter{
		tableName: tableName,
		Reader:    reader,
	}, nil
}

// Import imports data from Reader in BufferImporter.
func (i *BufferImporter) Import(db *DB, query string) (string, error) {
	ctx := context.Background()
	return i.ImportContext(ctx, db, query)
}

// ImportContext imports data from Reader in BufferImporter with context.
func (i *BufferImporter) ImportContext(ctx context.Context, db *DB, query string) (string, error) {
	names, err := i.Names()
	if err != nil {
		return query, err
	}
	types, err := i.Types()
	if err != nil {
		return query, err
	}
	if err := db.CreateTable(i.tableName, names, types, true); err != nil {
		return query, err
	}
	return query, db.ImportContext(ctx, i.tableName, names, i.Reader)
}
