package trdsql

import (
	"testing"
)

func TestNewExporter(t *testing.T) {
	type args struct {
		outFormat Format
	}
	tests := []struct {
		name string
		args args
		want Format
	}{
		{
			name: "test1",
			args: args{outFormat: CSV},
			want: CSV,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewExporter(NewWriter(OutFormat(tt.args.outFormat))); got == nil {
				t.Errorf("NewExporter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWriteFormat_Export(t *testing.T) {
	type fields struct {
		driver string
		dsn    string
	}
	type args struct {
		query string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:    "testErr",
			fields:  fields{driver: DefaultDriver, dsn: ""},
			args:    args{query: "SELECT 1 "},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Connect(tt.fields.driver, tt.fields.dsn)
			if err != nil {
				t.Fatal("Connect error")
			}
			e := NewExporter(nil)
			if err := e.Export(db, tt.args.query); (err != nil) != tt.wantErr {
				t.Errorf("WriteFormat.Export() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
