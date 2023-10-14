package trdsql

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestNewBufferImporter(t *testing.T) {
	type args struct {
		tableName string
		r         io.Reader
		options   []ReadOpt
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				tableName: "test",
				r:         bytes.NewBufferString("test"),
				options:   []ReadOpt{InFormat(CSV)},
			},
			want:    "test",
			wantErr: false,
		},
		{
			name: "testErr",
			args: args{
				tableName: "testErr",
				r:         bytes.NewBufferString("testErr"),
				options:   []ReadOpt{InFormat(VF)},
			},
			want:    "testErr",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewBufferImporter(tt.args.tableName, tt.args.r, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewBufferImporter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil {
				if !reflect.DeepEqual(got.tableName, tt.want) {
					t.Errorf("NewBufferImporter() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestBufferImporter_Import(t *testing.T) {
	type fields struct {
		tableName string
		r         io.Reader
	}
	tests := []struct {
		name    string
		fields  fields
		query   string
		want    string
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				tableName: "test",
				r:         bytes.NewBufferString("test"),
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
			reader, err := NewCSVReader(tt.fields.r, NewReadOpts())
			if err != nil {
				t.Fatal(err)
			}
			i := &BufferImporter{
				tableName: tt.fields.tableName,
				Reader:    reader,
			}
			got, err := i.Import(db, tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("BufferImporter.Import() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("BufferImporter.Import() = %v, want %v", got, tt.want)
			}
		})
	}
}
