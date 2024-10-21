package trdsql

import (
	"path/filepath"
	"reflect"
	"testing"
)

func Test_rowNumberReader_Names(t *testing.T) {
	type fields struct {
		reader    Reader
		originRow []any
		lineCount int
	}
	tests := []struct {
		name    string
		fields  fields
		want    []string
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				reader: &CSVReader{
					names: []string{"a", "b"},
					types: []string{"text", "text"},
					preRead: [][]string{
						{"1", "2"},
					},
				},
				originRow: []any{},
				lineCount: 0,
			},
			want:    []string{"num", "a", "b"},
			wantErr: false,
		},
		{
			name: "test2",
			fields: fields{
				reader: &CSVReader{
					names: []string{"num", "num1"},
					types: []string{"text", "text"},
					preRead: [][]string{
						{"1", "2"},
					},
				},
				originRow: []any{},
				lineCount: 0,
			},
			want:    []string{"num0", "num", "num1"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRowNumberReader(tt.fields.reader)
			got, err := r.Names()
			if (err != nil) != tt.wantErr {
				t.Errorf("rowNumberReader.Names() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("rowNumberReader.Names() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_rowNumberReader_Types(t *testing.T) {
	type fields struct {
		reader    Reader
		originRow []any
		lineCount int
	}
	tests := []struct {
		name    string
		fields  fields
		want    []string
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				reader: &CSVReader{
					names: []string{"a", "b"},
					types: []string{"text", "text"},
					preRead: [][]string{
						{"1", "2"},
					},
				},
				originRow: []any{},
				lineCount: 0,
			},
			want:    []string{"int", "text", "text"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRowNumberReader(tt.fields.reader)
			got, err := r.Types()
			if (err != nil) != tt.wantErr {
				t.Errorf("rowNumberReader.Types() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("rowNumberReader.Types() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_rowNumberReader_ReadRow(t *testing.T) {
	type args struct {
		row []any
	}
	tests := []struct {
		name     string
		fileName string
		opts     *ReadOpts
		args     args
		want1    [][]any
		want2    []any
		wantErr  bool
	}{
		{
			name:     "test.csv",
			fileName: "test.csv",
			opts:     NewReadOpts(),
			args:     args{row: []any{1}},
			want1: [][]any{
				{1, "1", "Orange"},
			},
			want2:   []any{2, "2", "Melon"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := singleFileOpen(filepath.Join(dataDir, tt.fileName))
			if err != nil {
				t.Error(err)
			}
			r, err := NewCSVReader(file, tt.opts)
			if err != nil {
				t.Error(err)
			}
			reader := newRowNumberReader(r)
			got1 := reader.PreReadRow()
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("rowNumberReader.PreReadRow() = %#v, want %#v", got1, tt.want1)
			}
			got2, err := reader.ReadRow(tt.args.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("rowNumberReader.ReadRow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got2, tt.want2) {
				t.Errorf("rowNumberReader.ReadRow() = %v, want %v", got2, tt.want2)
			}
		})
	}
}
