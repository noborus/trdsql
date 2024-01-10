package trdsql

import (
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNewLTSVReader(t *testing.T) {
	type args struct {
		reader io.Reader
		opts   *ReadOpts
	}
	tests := []struct {
		name    string
		args    args
		want    *LTSVReader
		wantErr bool
	}{
		{
			name: "empty",
			args: args{
				reader: strings.NewReader(""),
				opts:   NewReadOpts(),
			},
			want: &LTSVReader{
				names:   nil,
				types:   nil,
				preRead: nil,
			},
			wantErr: false,
		},
		{
			name: "oneLine",
			args: args{
				reader: strings.NewReader("ID:1\tname:test"),
				opts:   NewReadOpts(),
			},
			want: &LTSVReader{
				names:   []string{"ID", "name"},
				types:   []string{"text", "text"},
				preRead: []map[string]string{{"ID": "1", "name": "test"}},
			},
			wantErr: false,
		},
		{
			name: "invalidLTSV",
			args: args{
				reader: strings.NewReader("ID;1\tname:test"),
				opts:   NewReadOpts(),
			},
			want: &LTSVReader{
				names:   nil,
				types:   nil,
				preRead: nil,
			},
			wantErr: true,
		},
		{
			name: "diffColumn",
			args: args{
				reader: strings.NewReader("ID:1\tname:test\nID:2\tvalue:test"),
				opts:   NewReadOpts(InPreRead(2)),
			},
			want: &LTSVReader{
				names: []string{"ID", "name", "value"},
				types: []string{"text", "text", "text"},
				preRead: []map[string]string{
					{"ID": "1", "name": "test"},
					{"ID": "2", "value": "test"},
				},
			},
			wantErr: false,
		},
		{
			name: "ignoreColumn",
			args: args{
				reader: strings.NewReader("ID:1\tname:test\nID:2\tvalue:test"),
				opts:   NewReadOpts(InPreRead(1)),
			},
			want: &LTSVReader{
				names:   []string{"ID", "name"},
				types:   []string{"text", "text"},
				preRead: []map[string]string{{"ID": "1", "name": "test"}},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLTSVReader(tt.args.reader, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLTSVReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.names, tt.want.names) {
				t.Errorf("NewLTSVReader().names = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.types, tt.want.types) {
				t.Errorf("NewLTSVReader().types = %v, want %v", got.types, tt.want.types)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewLTSVReader().preRead = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}

func TestNewLTSVReaderFile(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		opts     *ReadOpts
		want     *LTSVReader
		wantErr  bool
	}{
		{
			name:     "test.ltsv",
			fileName: "test.ltsv",
			opts:     NewReadOpts(),
			want: &LTSVReader{
				names: []string{"id", "name", "price"},
				types: []string{"text", "text", "text"},
				preRead: []map[string]string{
					{"id": "1", "name": "Orange", "price": "50"},
				},
			},
			wantErr: false,
		},
		{
			name:     "test_indefinite",
			fileName: "test_indefinite.ltsv",
			opts:     NewReadOpts(),
			want: &LTSVReader{
				names: []string{"id", "name", "price"},
				types: []string{"text", "text", "text"},
				preRead: []map[string]string{
					{"id": "1", "name": "Orange", "price": "50"},
				},
			},
			wantErr: false,
		},
		{
			name:     "test_indefinite2",
			fileName: "test_indefinite.ltsv",
			opts: NewReadOpts(
				InPreRead(2),
			),
			want: &LTSVReader{
				names: []string{"id", "name", "price", "area"},
				types: []string{"text", "text", "text", "text"},
				preRead: []map[string]string{
					{"id": "1", "name": "Orange", "price": "50"},
					{"id": "2", "name": "Melon", "price": "500", "area": "ibaraki"},
				},
			},
			wantErr: false,
		},
		{
			name:     "test_indefinite3",
			fileName: "test_indefinite.ltsv",
			opts: NewReadOpts(
				InPreRead(100),
			),
			want: &LTSVReader{
				names: []string{"id", "name", "price", "area", "color"},
				types: []string{"text", "text", "text", "text", "text"},
				preRead: []map[string]string{
					{"id": "1", "name": "Orange", "price": "50"},
					{"id": "2", "name": "Melon", "price": "500", "area": "ibaraki"},
					{"id": "3", "name": "Apple", "price": "100", "area": "aomori", "color": "red"},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := singleFileOpen(filepath.Join(dataDir, tt.fileName))
			if err != nil {
				t.Error(err)
			}
			got, err := NewLTSVReader(file, tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLTSVReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.names, tt.want.names) {
				t.Errorf("NewLTSVReader().names = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.types, tt.want.types) {
				t.Errorf("NewLTSVReader().types = %v, want %v", got.types, tt.want.types)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewLTSVReader().preRead = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}

func TestLTSVReader_PreReadRow(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		opts     *ReadOpts
		want     [][]any
	}{
		{
			name:     "test1",
			fileName: "test_indefinite.ltsv",
			opts: NewReadOpts(
				InPreRead(100),
			),
			want: [][]any{
				{"1", "Orange", "50", "", ""},
				{"2", "Melon", "500", "ibaraki", ""},
				{"3", "Apple", "100", "aomori", "red"},
			},
		},
		{
			name:     "testNULL",
			fileName: "test_indefinite.ltsv",
			opts: NewReadOpts(
				InPreRead(100),
				InNeedNULL(true),
				InNULL(""),
			),
			want: [][]any{
				{"1", "Orange", "50", nil, nil},
				{"2", "Melon", "500", "ibaraki", nil},
				{"3", "Apple", "100", "aomori", "red"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := singleFileOpen(filepath.Join(dataDir, tt.fileName))
			if err != nil {
				t.Error(err)
			}
			r, err := NewLTSVReader(file, tt.opts)
			if err != nil {
				t.Error(err)
			}
			if got := r.PreReadRow(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LTSVReader.PreReadRow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLTSVReader_ReadRow(t *testing.T) {
	type args struct {
		row []any
	}
	tests := []struct {
		name     string
		fileName string
		opts     *ReadOpts
		args     args
		want     []any
		wantErr  bool
	}{
		{
			name:     "test1",
			fileName: "test_indefinite.ltsv",
			opts:     NewReadOpts(),
			args: args{
				[]any{
					"", "", "",
				},
			},
			want: []any{
				"2", "Melon", "500",
			},
			wantErr: false,
		},
		{
			name:     "testNULL",
			fileName: "testnull.ltsv",
			opts: NewReadOpts(
				InNeedNULL(true),
				InNULL(""),
			),
			args: args{
				[]any{
					"", "", "",
				},
			},
			want: []any{
				"2", nil, "500",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := singleFileOpen(filepath.Join(dataDir, tt.fileName))
			if err != nil {
				t.Error(err)
			}
			r, err := NewLTSVReader(file, tt.opts)
			if err != nil {
				t.Error(err)
			}
			got, err := r.ReadRow(tt.args.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("LTSVReader.ReadRow() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LTSVReader.ReadRow() = %v, want %v", got, tt.want)
			}
		})
	}
}
