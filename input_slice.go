package trdsql

import (
	"fmt"
	"io"
	"reflect"
)

type SliceReader struct {
	tableName string
	names     []string
	types     []string
	data      [][]interface{}
}

func NewSliceReader(tableName string, args interface{}) *SliceReader {
	val := reflect.ValueOf(args)

	if val.Kind() != reflect.Slice {
		single := val.Interface()
		data := [][]interface{}{
			{single},
		}
		names := []string{"c1"}
		types := []string{"text"}
		return &SliceReader{
			tableName: tableName,
			names:     names,
			types:     types,
			data:      data,
		}
	}

	var names []string
	var types []string
	var data [][]interface{}
	row := val.Index(0)
	switch row.Kind() {
	case reflect.Struct:
		length := val.Len()
		t := row.Type()
		columnNum := t.NumField()
		names = make([]string, columnNum)
		types = make([]string, columnNum)
		for i := 0; i < columnNum; i++ {
			f := t.Field(i)
			names[i] = f.Name
			types[i] = "text"
		}

		data = make([][]interface{}, 0)
		for i := 0; i < length; i++ {
			rows := val.Index(i)
			r := make([]interface{}, rows.NumField())
			for j := 0; j < rows.NumField(); j++ {
				r[j] = fmt.Sprintf("%v", rows.Field(j))
			}
			data = append(data, r)
		}
	case reflect.Slice:
		length := val.Len()
		columnNum := row.Len()
		names = make([]string, columnNum)
		types = make([]string, columnNum)
		for i := 0; i < columnNum; i++ {
			names[i] = fmt.Sprintf("c%d", i+1)
			types[i] = "text"
		}

		data = make([][]interface{}, 0)
		for i := 0; i < length; i++ {
			data = append(data, val.Index(i).Interface().([]interface{}))
		}
	default:
		single := val.Interface().([]interface{})
		names = []string{"c1"}
		types = []string{"text"}
		data = make([][]interface{}, 0)
		for i := 0; i < len(single); i++ {
			data = append(data, []interface{}{single[i]})
		}
	}

	return &SliceReader{
		tableName: tableName,
		names:     names,
		types:     types,
		data:      data,
	}
}

func (r *SliceReader) Names() ([]string, error) {
	return r.names, nil
}

func (r *SliceReader) Types() ([]string, error) {
	return r.types, nil
}

func (r *SliceReader) PreReadRow() [][]interface{} {
	return r.data
}

func (r *SliceReader) ReadRow(row []interface{}) ([]interface{}, error) {
	return nil, io.EOF
}
