package trdsql

import "context"

// SliceImporter includes SliceReader.
// SliceImporter can be used as a library from another program.
// It is not used from the command.
// SliceImporter is an importer that reads one slice data.
type SliceImporter struct {
	*SliceReader
}

// NewSliceImporter returns a SliceImporter.
func NewSliceImporter(tableName string, data any) *SliceImporter {
	return &SliceImporter{
		SliceReader: NewSliceReader(tableName, data),
	}
}

// Import imports data from SliceReader in SliceImporter.
func (i *SliceImporter) Import(db *DB, query string) (string, error) {
	ctx := context.Background()
	return i.ImportContext(ctx, db, query)
}

// ImportContext imports data from SliceReader in SliceImporter with context.
func (i *SliceImporter) ImportContext(ctx context.Context, db *DB, query string) (string, error) {
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
	return query, db.ImportContext(ctx, i.tableName, names, i.SliceReader)
}
