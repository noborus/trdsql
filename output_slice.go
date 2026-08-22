package trdsql

// SliceWriter stores query results in a slice.
type SliceWriter struct {
	Table [][]any
}

// NewSliceWriter returns a SliceWriter.
func NewSliceWriter() *SliceWriter {
	return &SliceWriter{}
}

// PreWrite initializes the result buffer.
func (w *SliceWriter) PreWrite(columns []string, types []string) error {
	w.Table = make([][]any, 0)
	return nil
}

// WriteRow appends one row to Table.
func (w *SliceWriter) WriteRow(values []any, columns []string) error {
	row := make([]any, len(values))
	copy(row, values)
	w.Table = append(w.Table, row)
	return nil
}

// PostWrite does nothing.
func (w *SliceWriter) PostWrite() error {
	return nil
}
