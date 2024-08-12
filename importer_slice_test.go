package trdsql

import (
	"context"
	"reflect"
	"testing"
)

func TestNewSliceImporter(t *testing.T) {
	type args struct {
		tableName string
		data      any
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test1",
			args: args{
				tableName: "test",
				data: [][]any{
					{1, "one"},
					{2, "two"},
					{3, "three"},
				},
			},
			want: "test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSliceImporter(tt.args.tableName, tt.args.data); !reflect.DeepEqual(got.tableName, tt.want) {
				t.Errorf("NewSliceImporter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSliceImporter_Import(t *testing.T) {
	type fields struct {
		tableName string
		data      any
	}
	tests := []struct {
		name    string
		fields  fields
		query   string
		want    string
		wantErr bool
	}{
		{
			name: "testErr",
			fields: fields{
				tableName: "",
				data: [][]any{
					{1, "one"},
					{2, "two"},
					{3, "three"},
				},
			},
			query:   "",
			want:    "",
			wantErr: true,
		},
		{
			name: "test1",
			fields: fields{
				tableName: "test",
				data: [][]any{
					{1, "one"},
					{2, "two"},
					{3, "three"},
				},
			},
			query:   "",
			want:    "",
			wantErr: false,
		},
		{
			name: "testNil",
			fields: fields{
				tableName: "testNil",
				data:      []int{},
			},
			query:   "",
			want:    "",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Connect(DefaultDriver, "")
			if err != nil {
				t.Fatal(err)
			}
			db.Tx, err = db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			i := NewSliceImporter(tt.fields.tableName, tt.fields.data)
			ctx := context.Background()
			got, err := i.Import(ctx, db, tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("SliceImporter.Import() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SliceImporter.Import() = %v, want %v", got, tt.want)
			}
		})
	}
}
