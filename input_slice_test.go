package trdsql

import (
	"reflect"
	"testing"
)

func TestNewSliceReader(t *testing.T) {
	type args struct {
		tableName string
		args      any
	}
	tests := []struct {
		name string
		args args
		want *SliceReader
	}{
		{
			name: "string",
			args: args{
				tableName: "one",
				args:      "one",
			},
			want: &SliceReader{
				tableName: "one",
				names:     []string{"c1"},
				types:     []string{"text"},
				data:      [][]any{{"one"}},
			},
		},
		{
			name: "int",
			args: args{
				tableName: "one",
				args:      1,
			},
			want: &SliceReader{
				tableName: "one",
				names:     []string{"c1"},
				types:     []string{"int"},
				data:      [][]any{{1}},
			},
		},
		{
			name: "single",
			args: args{
				tableName: "single",
				args:      []any{"a", "b"},
			},
			want: &SliceReader{
				tableName: "single",
				names:     []string{"c1"},
				types:     []string{"text"},
				data:      [][]any{{"a"}, {"b"}},
			},
		},
		{
			name: "singleInt",
			args: args{
				tableName: "singleInt",
				args:      []any{1, 2, 3},
			},
			want: &SliceReader{
				tableName: "singleInt",
				names:     []string{"c1"},
				types:     []string{"int"},
				data:      [][]any{{1}, {2}, {3}},
			},
		},
		{
			name: "slice",
			args: args{
				tableName: "slice",
				args: [][]any{
					{1, "one"},
					{2, "two"},
					{3, "three"},
				},
			},
			want: &SliceReader{
				tableName: "slice",
				names:     []string{"c1", "c2"},
				types:     []string{"int", "text"},
				data: [][]any{
					{1, "one"},
					{2, "two"},
					{3, "three"},
				},
			},
		},
		{
			name: "singleStruct",
			args: args{
				tableName: "struct",
				args: struct {
					id   int
					name string
				}{
					id: 1, name: "one",
				},
			},
			want: &SliceReader{
				tableName: "struct",
				names:     []string{"id", "name"},
				types:     []string{"int", "text"},
				data: [][]any{
					{"1", "one"},
				},
			},
		},
		{
			name: "struct",
			args: args{
				tableName: "struct",
				args: []struct {
					id   int
					name string
				}{
					{id: 1, name: "one"},
					{id: 2, name: "two"},
					{id: 3, name: "three"},
				},
			},
			want: &SliceReader{
				tableName: "struct",
				names:     []string{"id", "name"},
				types:     []string{"int", "text"},
				data: [][]any{
					{"1", "one"},
					{"2", "two"},
					{"3", "three"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSliceReader(tt.args.tableName, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSliceReader() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestNewMapSliceReader(t *testing.T) {
	type args struct {
		tableName string
		args      any
	}
	tests := []struct {
		name string
		args args
		want *SliceReader
	}{
		{
			name: "map",
			args: args{
				tableName: "map",
				args: map[string]string{
					"1": "one",
					"2": "two",
					"3": "three",
				},
			},
			want: &SliceReader{
				tableName: "map",
				names:     []string{"c1", "c2"},
				types:     []string{"text", "text"},
				data: [][]any{
					{"1", "one"},
					{"2", "two"},
					{"3", "three"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSliceReader(tt.args.tableName, tt.args.args); len(got.data) != len(tt.want.data) {
				t.Errorf("NewSliceReader() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestSliceReader_PreReadRow(t *testing.T) {
	type fields struct {
		tableName string
		names     []string
		types     []string
		data      [][]any
	}
	tests := []struct {
		name   string
		fields fields
		want   [][]any
	}{
		{
			name: "struct",
			fields: fields{
				tableName: "struct",
				names:     []string{"id", "name"},
				types:     []string{"text", "text"},
				data: [][]any{
					{"1", "one"},
					{"2", "two"},
					{"3", "three"},
				},
			},
			want: [][]any{
				{"1", "one"},
				{"2", "two"},
				{"3", "three"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &SliceReader{
				tableName: tt.fields.tableName,
				names:     tt.fields.names,
				types:     tt.fields.types,
				data:      tt.fields.data,
			}
			if got := r.PreReadRow(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SliceReader.PreReadRow() = %v, want %v", got, tt.want)
			}
		})
	}
}
