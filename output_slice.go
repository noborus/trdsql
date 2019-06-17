package trdsql

type SliceWriter struct {
	Table [][]interface{}
}

func NewSliceWriter() *SliceWriter {
	return &SliceWriter{}
}

func (w *SliceWriter) PreWrite(columns []string, types []string) error {
	w.Table = make([][]interface{}, 0)
	return nil
}
func (w *SliceWriter) WriteRow(values []interface{}, columns []string) error {
	row := make([]interface{}, len(values))
	for i, v := range values {
		row[i] = ValString(v)
	}
	w.Table = append(w.Table, row)
	return nil
}
func (w *SliceWriter) PostWrite() error {
	return nil
}
