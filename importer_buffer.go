package trdsql

import (
	"io"
)

// BufferImporter a structure that includes tableName and Reader.
type BufferImporter struct {
	tableName string
	Reader
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

// Import is a method to import from Reader in BufferImporter.
func (i *BufferImporter) Import(db *DB, query string) (string, error) {
	names, err := i.Names()
	if err != nil {
		return query, err
	}
	types, err := i.Types()
	if err != nil {
		return query, err
	}
	err = db.CreateTable(i.tableName, names, types, true)
	if err != nil {
		return query, err
	}
	err = db.Import(i.tableName, names, i.Reader)
	return query, err
}
