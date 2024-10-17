package trdsql

import (
	"strconv"
)

// rowNumberReader is a wrapper around a Reader that transforms.
type rowNumberReader struct {
	reader    Reader
	originRow []any
	lineCount int
}

// newRowNumberReader creates a new TransformReader.
func newRowNumberReader(r Reader) *rowNumberReader {
	columnNum := 1
	names, err := r.Names()
	if err == nil {
		columnNum = len(names)
	}
	originRow := make([]any, columnNum)
	return &rowNumberReader{
		reader:    r,
		originRow: originRow,
		lineCount: 0,
	}
}

// Names returns column names with an additional row number column.
func (r *rowNumberReader) Names() ([]string, error) {
	number := "num"
	names, err := r.reader.Names()
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(names)+1; i++ {
		orig := number
		for _, name := range names {
			if number == name {
				number = orig + strconv.Itoa(i)
				i++
				continue
			}
		}
	}

	return append([]string{number}, names...), nil
}

// Types returns column types with an additional row number column.
func (r *rowNumberReader) Types() ([]string, error) {
	types, err := r.reader.Types()
	if err != nil {
		return nil, err
	}
	return append([]string{"int"}, types...), nil
}

// PreReadRow returns pre-read rows with an additional row number column.
func (r *rowNumberReader) PreReadRow() [][]any {
	preReadRows := r.reader.PreReadRow()
	for i := range preReadRows {
		preReadRows[i] = append([]any{r.lineCount + i + 1}, preReadRows[i]...)
	}
	r.lineCount += len(preReadRows)
	return preReadRows
}

// ReadRow reads the rest of the row with an additional row number column.
func (r *rowNumberReader) ReadRow(row []any) ([]any, error) {
	var err error
	r.lineCount++
	r.originRow, err = r.reader.ReadRow(r.originRow)
	if err != nil {
		return nil, err
	}
	if len(r.originRow) == 0 {
		return nil, nil
	}

	return append([]any{r.lineCount}, r.originRow...), nil
}
