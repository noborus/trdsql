package trdsql

import (
	"fmt"
	"io"
	"reflect"
)

// SliceReader is a structure for reading tabular data in memory.
// It can be used as the trdsql reader interface.
type SliceReader struct {
	tableName string
	names     []string
	types     []string
	data      [][]interface{}
}

// NewSliceReader takes a tableName and tabular data in memory
// and returns SliceReader.
// The tabular data that can be received is
// a one-dimensional array,
// a two-dimensional array,
// a map,
// and an array of structures.
func NewSliceReader(tableName string, args interface{}) *SliceReader {
	val := reflect.ValueOf(args)
	if val.Kind() == reflect.Ptr {
		val = reflect.Indirect(val)
	}

	// One-dimensional
	switch val.Kind() {
	case reflect.Map:
		return mapReader(tableName, val)
	case reflect.Struct:
		return structReader(tableName, val)
	case reflect.Slice:
		return sliceReader(tableName, val)
	default:
		single := val.Interface()
		data := [][]interface{}{
			{single},
		}
		names := []string{"c1"}
		types := []string{typeToDBType(val.Kind())}
		return &SliceReader{
			tableName: tableName,
			names:     names,
			types:     types,
			data:      data,
		}
	}
}

func mapReader(tableName string, val reflect.Value) *SliceReader {
	val = reflect.Indirect(val)
	names := []string{"c1", "c2"}
	keyType := val.MapKeys()[0].Kind()
	valType := val.MapIndex(val.MapKeys()[0]).Kind()
	types := []string{typeToDBType(keyType), typeToDBType(valType)}
	data := make([][]interface{}, 0)
	for _, e := range val.MapKeys() {
		data = append(data, []interface{}{e.Interface(), val.MapIndex(e).Interface()})
	}
	return &SliceReader{
		tableName: tableName,
		names:     names,
		types:     types,
		data:      data,
	}
}

func structReader(tableName string, val reflect.Value) *SliceReader {
	t := val.Type()
	columnNum := t.NumField()
	names := make([]string, columnNum)
	types := make([]string, columnNum)
	for i := 0; i < columnNum; i++ {
		f := t.Field(i)
		names[i] = f.Name
		types[i] = typeToDBType(f.Type.Kind())
	}
	single := make([]interface{}, t.NumField())
	for j := 0; j < t.NumField(); j++ {
		single[j] = fmt.Sprintf("%v", val.Field(j))
	}
	data := [][]interface{}{
		single,
	}

	return &SliceReader{
		tableName: tableName,
		names:     names,
		types:     types,
		data:      data,
	}
}

// Two-dimensional
func sliceReader(tableName string, val reflect.Value) *SliceReader {
	if val.Len() == 0 {
		return &SliceReader{
			tableName: tableName,
			names:     []string{"c1"},
			types:     []string{"text"},
			data:      nil,
		}
	}
	switch val.Index(0).Kind() {
	case reflect.Struct:
		// {{ id: 1, name: "test"},{ id: 2, name: "test2"}}
		return structSliceReader(tableName, val)
	case reflect.Slice:
		// {{1, "test"},{2, "test2"}}
		return sliceSliceReader(tableName, val)
	default:
		// {{"a", "b", "c"}}
		return interfaceSliceReader(tableName, val)
	}
}

func structSliceReader(tableName string, val reflect.Value) *SliceReader {
	length := val.Len()
	t := val.Index(0).Type()
	columnNum := t.NumField()
	names := make([]string, columnNum)
	types := make([]string, columnNum)
	for i := 0; i < columnNum; i++ {
		f := t.Field(i)
		names[i] = f.Name
		types[i] = typeToDBType(f.Type.Kind())
	}
	data := make([][]interface{}, 0)
	for i := 0; i < length; i++ {
		rows := val.Index(i)
		r := make([]interface{}, rows.NumField())
		for j := 0; j < rows.NumField(); j++ {
			r[j] = fmt.Sprintf("%v", rows.Field(j))
		}
		data = append(data, r)
	}
	return &SliceReader{
		tableName: tableName,
		names:     names,
		types:     types,
		data:      data,
	}
}

func sliceSliceReader(tableName string, val reflect.Value) *SliceReader {
	length := val.Len()
	col := val.Index(0)
	columnNum := col.Len()
	names := make([]string, columnNum)
	types := make([]string, columnNum)
	for i := 0; i < columnNum; i++ {
		names[i] = fmt.Sprintf("c%d", i+1)
		colType := reflect.ValueOf(col.Index(i).Interface()).Kind()
		types[i] = typeToDBType(colType)
	}

	data := make([][]interface{}, 0)
	for i := 0; i < length; i++ {
		data = append(data, val.Index(i).Interface().([]interface{}))
	}
	return &SliceReader{
		tableName: tableName,
		names:     names,
		types:     types,
		data:      data,
	}
}

func interfaceSliceReader(tableName string, val reflect.Value) *SliceReader {
	v := val.Index(0).Interface()
	length := val.Len()
	t := reflect.ValueOf(v)
	names := []string{"c1"}
	types := []string{typeToDBType(t.Kind())}
	data := make([][]interface{}, length)
	for i := 0; i < length; i++ {
		data[i] = []interface{}{val.Index(i).Interface()}
	}
	return &SliceReader{
		tableName: tableName,
		names:     names,
		types:     types,
		data:      data,
	}
}

// In sliceReader, only int type is passed to the database as int type.
func typeToDBType(t reflect.Kind) string {
	switch t {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "int"
	default:
		return DefaultDBType
	}
}

// TableName returns Table name.
func (r *SliceReader) TableName() (string, error) {
	return r.tableName, nil
}

// Names returns column names.
func (r *SliceReader) Names() ([]string, error) {
	return r.names, nil
}

// Types returns column types.
func (r *SliceReader) Types() ([]string, error) {
	return r.types, nil
}

// PreReadRow is returns entity of the data.
func (r *SliceReader) PreReadRow() [][]interface{} {
	return r.data
}

// ReadRow only returns EOF.
func (r *SliceReader) ReadRow(row []interface{}) ([]interface{}, error) {
	return nil, io.EOF
}
