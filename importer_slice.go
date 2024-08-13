package trdsql

import "context"

// SliceImporter is a structure that includes SliceReader.
// SliceImporter can be used as a library from another program.
// It is not used from the command.
// SliceImporter is an importer that reads one slice data.
type SliceImporter struct {
	*SliceReader
}

// NewSliceImporter returns trdsql SliceImporter.
func NewSliceImporter(tableName string, data any) *SliceImporter {
	return &SliceImporter{
		SliceReader: NewSliceReader(tableName, data),
	}
}

// ImportContext is a method to import from SliceReader in SliceImporter.
func (i *SliceImporter) Import(ctx context.Context, db *DB, query string) (string, error) {
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
	return query, db.Import(ctx, i.tableName, names, i.SliceReader)
}
