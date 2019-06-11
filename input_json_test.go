package trdsql

import (
	"encoding/json"
	"io"
	"reflect"
	"testing"
)

func TestNewJSONReader(t *testing.T) {
	type args struct {
		reader io.Reader
	}
	tests := []struct {
		name    string
		args    args
		want    *JSONReader
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewJSONReader(tt.args.reader)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewJSONReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewJSONReader() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJSONReader_GetColumn(t *testing.T) {
	type fields struct {
		reader  *json.Decoder
		preRead []map[string]string
		names   []string
		types   []string
		inArray []interface{}
		count   int
	}
	type args struct {
		rowNum int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &JSONReader{
				reader:  tt.fields.reader,
				preRead: tt.fields.preRead,
				names:   tt.fields.names,
				types:   tt.fields.types,
				inArray: tt.fields.inArray,
				count:   tt.fields.count,
			}
			got, err := r.GetColumn(tt.args.rowNum)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONReader.GetColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JSONReader.GetColumn() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJSONReader_GetTypes(t *testing.T) {
	type fields struct {
		reader  *json.Decoder
		preRead []map[string]string
		names   []string
		types   []string
		inArray []interface{}
		count   int
	}
	tests := []struct {
		name    string
		fields  fields
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &JSONReader{
				reader:  tt.fields.reader,
				preRead: tt.fields.preRead,
				names:   tt.fields.names,
				types:   tt.fields.types,
				inArray: tt.fields.inArray,
				count:   tt.fields.count,
			}
			got, err := r.GetTypes()
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONReader.GetTypes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JSONReader.GetTypes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJSONReader_readAhead(t *testing.T) {
	type fields struct {
		reader  *json.Decoder
		preRead []map[string]string
		names   []string
		types   []string
		inArray []interface{}
		count   int
	}
	type args struct {
		top   interface{}
		count int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]string
		want1   []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &JSONReader{
				reader:  tt.fields.reader,
				preRead: tt.fields.preRead,
				names:   tt.fields.names,
				types:   tt.fields.types,
				inArray: tt.fields.inArray,
				count:   tt.fields.count,
			}
			got, got1, err := r.readAhead(tt.args.top, tt.args.count)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONReader.readAhead() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JSONReader.readAhead() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("JSONReader.readAhead() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestJSONReader_topLevel(t *testing.T) {
	type fields struct {
		reader  *json.Decoder
		preRead []map[string]string
		names   []string
		types   []string
		inArray []interface{}
		count   int
	}
	type args struct {
		top interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]string
		want1   []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &JSONReader{
				reader:  tt.fields.reader,
				preRead: tt.fields.preRead,
				names:   tt.fields.names,
				types:   tt.fields.types,
				inArray: tt.fields.inArray,
				count:   tt.fields.count,
			}
			got, got1, err := r.topLevel(tt.args.top)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONReader.topLevel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JSONReader.topLevel() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("JSONReader.topLevel() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestJSONReader_secondLevel(t *testing.T) {
	type fields struct {
		reader  *json.Decoder
		preRead []map[string]string
		names   []string
		types   []string
		inArray []interface{}
		count   int
	}
	type args struct {
		top    interface{}
		second interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]string
		want1   []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &JSONReader{
				reader:  tt.fields.reader,
				preRead: tt.fields.preRead,
				names:   tt.fields.names,
				types:   tt.fields.types,
				inArray: tt.fields.inArray,
				count:   tt.fields.count,
			}
			got, got1, err := r.secondLevel(tt.args.top, tt.args.second)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONReader.secondLevel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JSONReader.secondLevel() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("JSONReader.secondLevel() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestJSONReader_objectFirstRow(t *testing.T) {
	type fields struct {
		reader  *json.Decoder
		preRead []map[string]string
		names   []string
		types   []string
		inArray []interface{}
		count   int
	}
	type args struct {
		obj map[string]interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]string
		want1   []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &JSONReader{
				reader:  tt.fields.reader,
				preRead: tt.fields.preRead,
				names:   tt.fields.names,
				types:   tt.fields.types,
				inArray: tt.fields.inArray,
				count:   tt.fields.count,
			}
			got, got1, err := r.objectFirstRow(tt.args.obj)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONReader.objectFirstRow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JSONReader.objectFirstRow() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("JSONReader.objectFirstRow() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestJSONReader_etcFirstRow(t *testing.T) {
	type fields struct {
		reader  *json.Decoder
		preRead []map[string]string
		names   []string
		types   []string
		inArray []interface{}
		count   int
	}
	type args struct {
		val interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]string
		want1   []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &JSONReader{
				reader:  tt.fields.reader,
				preRead: tt.fields.preRead,
				names:   tt.fields.names,
				types:   tt.fields.types,
				inArray: tt.fields.inArray,
				count:   tt.fields.count,
			}
			got, got1, err := r.etcFirstRow(tt.args.val)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONReader.etcFirstRow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JSONReader.etcFirstRow() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("JSONReader.etcFirstRow() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_jsonString(t *testing.T) {
	type args struct {
		val interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := jsonString(tt.args.val); got != tt.want {
				t.Errorf("jsonString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJSONReader_PreReadRow(t *testing.T) {
	type fields struct {
		reader  *json.Decoder
		preRead []map[string]string
		names   []string
		types   []string
		inArray []interface{}
		count   int
	}
	tests := []struct {
		name   string
		fields fields
		want   [][]interface{}
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &JSONReader{
				reader:  tt.fields.reader,
				preRead: tt.fields.preRead,
				names:   tt.fields.names,
				types:   tt.fields.types,
				inArray: tt.fields.inArray,
				count:   tt.fields.count,
			}
			if got := r.PreReadRow(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JSONReader.PreReadRow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJSONReader_ReadRow(t *testing.T) {
	type fields struct {
		reader  *json.Decoder
		preRead []map[string]string
		names   []string
		types   []string
		inArray []interface{}
		count   int
	}
	type args struct {
		row []interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &JSONReader{
				reader:  tt.fields.reader,
				preRead: tt.fields.preRead,
				names:   tt.fields.names,
				types:   tt.fields.types,
				inArray: tt.fields.inArray,
				count:   tt.fields.count,
			}
			got, err := r.ReadRow(tt.args.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONReader.ReadRow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JSONReader.ReadRow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJSONReader_rowParse(t *testing.T) {
	type fields struct {
		reader  *json.Decoder
		preRead []map[string]string
		names   []string
		types   []string
		inArray []interface{}
		count   int
	}
	type args struct {
		row     []interface{}
		jsonRow interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []interface{}
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &JSONReader{
				reader:  tt.fields.reader,
				preRead: tt.fields.preRead,
				names:   tt.fields.names,
				types:   tt.fields.types,
				inArray: tt.fields.inArray,
				count:   tt.fields.count,
			}
			if got := r.rowParse(tt.args.row, tt.args.jsonRow); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JSONReader.rowParse() = %v, want %v", got, tt.want)
			}
		})
	}
}
