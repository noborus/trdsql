package trdsql

// SliceWriter is a structure to receive the result in slice.
type SliceWriter struct {
	Table [][]interface{}
}

// NewSliceWriter return SliceWriter.
func NewSliceWriter() *SliceWriter {
	return &SliceWriter{}
}

// PreWrite prepares the area.
func (w *SliceWriter) PreWrite(columns []string, types []string) error {
	w.Table = make([][]interface{}, 0)
	return nil
}

// WriteRow stores the result in Table.
func (w *SliceWriter) WriteRow(values []interface{}, columns []string) error {
	row := make([]interface{}, len(values))
	copy(row, values)
	w.Table = append(w.Table, row)
	return nil
}

// PostWrite does nothing.
func (w *SliceWriter) PostWrite() error {
	return nil
}
