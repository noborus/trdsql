package trdsql

import (
	"context"
	"io"
)

// BufferImporter a structure that includes tableName and Reader.
type BufferImporter struct {
	Reader
	tableName string
}

// NewBufferImporter returns trdsql BufferImporter.
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

// ImportContext is a method to import from Reader in BufferImporter.
func (i *BufferImporter) Import(ctx context.Context, db *DB, query string) (string, error) {
	names, err := i.Names()
	if err != nil {
		return query, err
	}
	types, err := i.Types()
	if err != nil {
		return query, err
	}
	if err := db.CreateTable(ctx, i.tableName, names, types, true); err != nil {
		return query, err
	}
	return query, db.Import(ctx, i.tableName, names, i.Reader)
}
