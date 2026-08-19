package trdsql

import (
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNewLogfmtReader(t *testing.T) {
	type args struct {
		reader io.Reader
		opts   *ReadOpts
	}
	tests := []struct {
		name    string
		args    args
		want    *LogfmtReader
		wantErr bool
	}{
		{
			name: "empty",
			args: args{
				reader: strings.NewReader(""),
				opts:   NewReadOpts(),
			},
			want: &LogfmtReader{
				names:   nil,
				types:   nil,
				preRead: nil,
			},
			wantErr: false,
		},
		{
			name: "oneLine",
			args: args{
				reader: strings.NewReader("ID=1 name=test"),
				opts:   NewReadOpts(),
			},
			want: &LogfmtReader{
				names:   []string{"ID", "name"},
				types:   []string{"text", "text"},
				preRead: []map[string]string{{"ID": "1", "name": "test"}},
			},
			wantErr: false,
		},
		{
			name: "quotedValue",
			args: args{
				reader: strings.NewReader(`ID=1 name="Blood Orange" note="say \"hi\""`),
				opts:   NewReadOpts(),
			},
			want: &LogfmtReader{
				names:   []string{"ID", "name", "note"},
				types:   []string{"text", "text", "text"},
				preRead: []map[string]string{{"ID": "1", "name": "Blood Orange", "note": `say "hi"`}},
			},
			wantErr: false,
		},
		{
			name: "bareKey",
			args: args{
				reader: strings.NewReader("ID=1 active msg=ok"),
				opts:   NewReadOpts(),
			},
			want: &LogfmtReader{
				names:   []string{"ID", "active", "msg"},
				types:   []string{"text", "text", "text"},
				preRead: []map[string]string{{"ID": "1", "active": "", "msg": "ok"}},
			},
			wantErr: false,
		},
		{
			name: "unterminatedQuote",
			args: args{
				reader: strings.NewReader(`ID=1 name="oops`),
				opts:   NewReadOpts(),
			},
			want: &LogfmtReader{
				names:   nil,
				types:   nil,
				preRead: nil,
			},
			wantErr: true,
		},
		{
			name: "diffColumn",
			args: args{
				reader: strings.NewReader("ID=1 name=test\nID=2 value=test"),
				opts:   NewReadOpts(InPreRead(2)),
			},
			want: &LogfmtReader{
				names: []string{"ID", "name", "value"},
				types: []string{"text", "text", "text"},
				preRead: []map[string]string{
					{"ID": "1", "name": "test"},
					{"ID": "2", "value": "test"},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLogfmtReader(tt.args.reader, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLogfmtReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.names, tt.want.names) {
				t.Errorf("NewLogfmtReader().names = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.types, tt.want.types) {
				t.Errorf("NewLogfmtReader().types = %v, want %v", got.types, tt.want.types)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewLogfmtReader().preRead = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}

func TestNewLogfmtReaderFile(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		opts     *ReadOpts
		want     *LogfmtReader
		wantErr  bool
	}{
		{
			name:     "test.logfmt",
			fileName: "test.logfmt",
			opts:     NewReadOpts(),
			want: &LogfmtReader{
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
			fileName: "test_indefinite.logfmt",
			opts: NewReadOpts(
				InPreRead(2),
			),
			want: &LogfmtReader{
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
			fileName: "test_indefinite.logfmt",
			opts: NewReadOpts(
				InPreRead(100),
			),
			want: &LogfmtReader{
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
		{
			name:     "test_quote",
			fileName: "test_quote.logfmt",
			opts:     NewReadOpts(),
			want: &LogfmtReader{
				names: []string{"id", "name", "note"},
				types: []string{"text", "text", "text"},
				preRead: []map[string]string{
					{"id": "1", "name": "Blood Orange", "note": `say "hi"`},
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
			got, err := NewLogfmtReader(file, tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLogfmtReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.names, tt.want.names) {
				t.Errorf("NewLogfmtReader().names = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.types, tt.want.types) {
				t.Errorf("NewLogfmtReader().types = %v, want %v", got.types, tt.want.types)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewLogfmtReader().preRead = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}

func TestLogfmtReader_PreReadRow(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		opts     *ReadOpts
		want     [][]any
	}{
		{
			name:     "test1",
			fileName: "test_indefinite.logfmt",
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
			fileName: "test_indefinite.logfmt",
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
			r, err := NewLogfmtReader(file, tt.opts)
			if err != nil {
				t.Error(err)
			}
			if got := r.PreReadRow(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LogfmtReader.PreReadRow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogfmtReader_ReadRow(t *testing.T) {
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
			fileName: "test_indefinite.logfmt",
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
			fileName: "testnull.logfmt",
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
			r, err := NewLogfmtReader(file, tt.opts)
			if err != nil {
				t.Error(err)
			}
			got, err := r.ReadRow(tt.args.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("LogfmtReader.ReadRow() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LogfmtReader.ReadRow() = %v, want %v", got, tt.want)
			}
		})
	}
}
