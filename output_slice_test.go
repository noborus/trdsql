package trdsql

import (
	"reflect"
	"testing"
)

func TestNewSliceWriter(t *testing.T) {
	tests := []struct {
		name string
		want *SliceWriter
	}{
		{
			name: "test1",
			want: &SliceWriter{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSliceWriter(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSliceWriter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSliceWriter_PreWrite(t *testing.T) {
	type fields struct {
		Table [][]interface{}
	}
	type args struct {
		columns []string
		types   []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				Table: [][]interface{}{
					{"1", "one"},
					{"2", "two"},
					{"3", "three"},
				},
			},
			args:    args{columns: nil, types: nil},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &SliceWriter{
				Table: tt.fields.Table,
			}
			if err := w.PreWrite(tt.args.columns, tt.args.types); (err != nil) != tt.wantErr {
				t.Errorf("SliceWriter.PreWrite() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSliceWriter_WriteRow(t *testing.T) {
	type fields struct {
		Table [][]interface{}
	}
	type args struct {
		values  []interface{}
		columns []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				Table: [][]interface{}{
					{"1", "one"},
					{"2", "two"},
					{"3", "three"},
				},
			},
			args: args{
				values: []interface{}{
					"", "",
				},
				columns: []string{"id", "name"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &SliceWriter{
				Table: tt.fields.Table,
			}
			if err := w.WriteRow(tt.args.values, tt.args.columns); (err != nil) != tt.wantErr {
				t.Errorf("SliceWriter.WriteRow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
