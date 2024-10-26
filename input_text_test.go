package trdsql

import (
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNewTextReader(t *testing.T) {
	type args struct {
		reader io.Reader
		opts   *ReadOpts
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test1",
			args: args{
				reader: strings.NewReader("a\nb\nc\n"),
				opts:   NewReadOpts(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewTextReader(tt.args.reader, tt.args.opts)
			if err != nil {
				t.Fatal(err)
			}
			names, err := got.Names()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(names, []string{"text"}) {
				t.Errorf("TextReader.Names() != text %v", names)
			}
			types, err := got.Types()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(types, []string{"text"}) {
				t.Errorf("TextReader.Types() != text %v", types)
			}
		})
	}
}

func TestTextReaderFile(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		opts     *ReadOpts
		want     []any
		wantErr  bool
	}{
		{
			name:     "test.csv",
			fileName: "test.csv",
			opts:     NewReadOpts(),
			want:     []any{"1,Orange"},
			wantErr:  false,
		},
		{
			name:     "test.csv2",
			fileName: "test.csv",
			opts:     &ReadOpts{InSkip: 1},
			want:     []any{"2,Melon"},
			wantErr:  false,
		},
		{
			name:     "test.csv3",
			fileName: "test.csv",
			opts:     &ReadOpts{InLimitRead: true, InPreRead: 1},
			want:     []any{"1,Orange"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := singleFileOpen(filepath.Join(dataDir, tt.fileName))
			if err != nil {
				t.Error(err)
			}
			r, err := NewTextReader(file, tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			got, err := r.ReadRow(nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("TextReader.ReadRow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TextReader.ReadRow() = %v, want %v", got, tt.want)
			}
		})
	}
}
