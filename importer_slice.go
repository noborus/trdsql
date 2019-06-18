package trdsql

type SliceImporter struct {
	*SliceReader
}

func NewSliceImporter(tableName string, data interface{}) *SliceImporter {
	return &SliceImporter{
		SliceReader: NewSliceReader(tableName, data),
	}
}

func (i *SliceImporter) Import(db *DB, query string) (string, error) {
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
	err = db.Import(i.tableName, names, i.SliceReader)
	return query, err
}
