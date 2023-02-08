package trdsql

import (
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
)

func TestNewCSVReader(t *testing.T) {
	type args struct {
		reader io.Reader
		opts   *ReadOpts
	}
	tests := []struct {
		name    string
		args    args
		want    *CSVReader
		wantErr bool
	}{
		{
			name: "empty",
			args: args{
				reader: strings.NewReader(""),
				opts:   NewReadOpts(),
			},
			want: &CSVReader{
				names:   nil,
				types:   nil,
				preRead: nil,
			},
			wantErr: false,
		},
		{
			name: "header",
			args: args{
				reader: strings.NewReader("he1,he2\nv1,v2"),
				opts: NewReadOpts(
					InHeader(true),
					InDelimiter(","),
					InPreRead(1),
				),
			},
			want: &CSVReader{
				names:   []string{"he1", "he2"},
				types:   []string{"text", "text"},
				preRead: nil,
			},
			wantErr: false,
		},
		{
			name: "notEnoughHeader",
			args: args{
				reader: strings.NewReader("h1,\nv1,v2"),
				opts: NewReadOpts(
					InHeader(true),
					InDelimiter(","),
					InPreRead(2),
				),
			},
			want: &CSVReader{
				names:   []string{"h1", "c2"},
				types:   []string{"text", "text"},
				preRead: [][]string{{"v1", "v2"}},
			},
			wantErr: false,
		},
		{
			name: "headerOnly",
			args: args{
				reader: strings.NewReader("h1,h2\n"),
				opts: NewReadOpts(
					InHeader(true),
					InDelimiter(","),
					InPreRead(2),
				),
			},
			want: &CSVReader{
				names:   []string{"h1", "h2"},
				types:   []string{"text", "text"},
				preRead: nil,
			},
			wantErr: false,
		},
		{
			name: "spaceDelimiter",
			args: args{
				reader: iotest.DataErrReader(strings.NewReader("h1 h2\nv1 v2\n")),
				opts: NewReadOpts(
					InHeader(true),
					InDelimiter(" "),
					InPreRead(2),
				),
			},
			want: &CSVReader{
				names:   []string{"h1", "h2"},
				types:   []string{"text", "text"},
				preRead: [][]string{{"v1", "v2"}},
			},
			wantErr: false,
		},
		{
			name: "spaceTrim",
			args: args{
				reader: iotest.DataErrReader(strings.NewReader("h1      h2\nv1 v2\n")),
				opts: NewReadOpts(
					InHeader(true),
					InDelimiter(" "),
					InPreRead(2),
				),
			},
			want: &CSVReader{
				names:   []string{"h1", "h2"},
				types:   []string{"text", "text"},
				preRead: [][]string{{"v1", "v2"}},
			},
			wantErr: false,
		},
		{
			name: "tabDelimiter",
			args: args{
				reader: iotest.DataErrReader(strings.NewReader("h1\th2\nv1\tv2\n")),
				opts: NewReadOpts(
					InHeader(true),
					InDelimiter("\t"),
					InPreRead(2),
				),
			},
			want: &CSVReader{
				names:   []string{"h1", "h2"},
				types:   []string{"text", "text"},
				preRead: [][]string{{"v1", "v2"}},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewCSVReader(tt.args.reader, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCSVReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.names, tt.want.names) {
				t.Errorf("NewCSVReader().names = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.types, tt.want.types) {
				t.Errorf("NewCSVReader().types = %v, want %v", got.types, tt.want.types)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewCSVReader().preRead = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}

func TestNewCSVReaderFile(t *testing.T) {
	type args struct {
		opts *ReadOpts
	}
	tests := []struct {
		name     string
		fileName string
		args     args
		want     *CSVReader
		wantLen  int
		wantErr  bool
	}{
		{
			name:     "test.csv",
			fileName: "test.csv",
			args:     args{opts: NewReadOpts()},
			want: &CSVReader{
				names:   []string{"c1", "c2"},
				types:   []string{"text", "text"},
				preRead: [][]string{{"1", "Orange"}},
			},
			wantLen: 2,
			wantErr: false,
		},
		{
			name:     "test_indefinite.csv",
			fileName: "test_indefinite.csv",
			args:     args{opts: NewReadOpts()},
			want: &CSVReader{
				names:   []string{"c1", "c2"},
				types:   []string{"text", "text"},
				preRead: [][]string{{"1", "Orange"}},
			},
			wantLen: 2,
			wantErr: false,
		},
		{
			name:     "test_indefinite.csv2",
			fileName: "test_indefinite.csv",
			args: args{opts: NewReadOpts(
				InPreRead(2),
			)},
			want: &CSVReader{
				names: []string{"c1", "c2", "c3"},
				types: []string{"text", "text", "text"},
				preRead: [][]string{
					{"1", "Orange"},
					{"2", "Melon", "ibraki"},
				},
			},
			wantLen: 2,
			wantErr: false,
		},
		{
			name:     "test_indefinite.csv3",
			fileName: "test_indefinite.csv",
			args: args{opts: NewReadOpts(
				InPreRead(100),
			)},
			want: &CSVReader{
				names: []string{"c1", "c2", "c3", "c4"},
				types: []string{"text", "text", "text", "text"},
				preRead: [][]string{
					{"1", "Orange"},
					{"2", "Melon", "ibraki"},
					{"3", "Apple", "aomori", "red"},
				},
			},
			wantLen: 2,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := singleFileOpen(filepath.Join(dataDir, tt.fileName))
			if err != nil {
				t.Error(err)
			}
			got, err := NewCSVReader(file, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCSVReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.names, tt.want.names) {
				t.Errorf("NewCSVReader().names = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.types, tt.want.types) {
				t.Errorf("NewCSVReader().types = %v, want %v", got.types, tt.want.types)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewCSVReader().preRead = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}

func Test_delimiter(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		want    rune
		wantErr bool
	}{
		{
			name:    "comma",
			args:    ",",
			want:    rune(','),
			wantErr: false,
		},
		{
			name:    "multi",
			args:    "--",
			want:    rune(','),
			wantErr: true,
		},
		{
			name:    "zero",
			args:    "",
			want:    rune(0),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := delimiter(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("delimiter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("delimiter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCSVReader_PreReadRow(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		opts     *ReadOpts
		want     [][]interface{}
	}{
		{
			name:     "testBlank",
			fileName: "testnull.csv",
			opts:     NewReadOpts(),
			want: [][]interface{}{
				{"1", ""},
			},
		},
		{
			name:     "testnull1",
			fileName: "testnull.csv",
			opts: NewReadOpts(
				InNeedNULL(true),
				InNULL(""),
			),
			want: [][]interface{}{
				{"1", nil},
			},
		},
		{
			name:     "testnull2",
			fileName: "testnull.csv",
			opts: NewReadOpts(
				InNeedNULL(true),
				InNULL("1"),
			),
			want: [][]interface{}{
				{nil, ""},
			},
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
			if got := r.PreReadRow(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CSVReader.PreReadRow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCSVReader_ReadRow(t *testing.T) {
	type args struct {
		row []interface{}
	}
	tests := []struct {
		name     string
		fileName string
		opts     *ReadOpts
		args     args
		want     []interface{}
		wantErr  bool
	}{
		{
			name:     "test.csv",
			fileName: "test.csv",
			opts:     NewReadOpts(),
			args: args{
				[]interface{}{
					"", "",
				},
			},
			want: []interface{}{
				"2", "Melon",
			},
			wantErr: false,
		},
		{
			name:     "testblank",
			fileName: "testnull2.csv",
			opts:     NewReadOpts(),
			args: args{
				[]interface{}{
					"", "", "",
				},
			},
			want: []interface{}{
				"2", "", "g3",
			},
			wantErr: false,
		},
		{
			name:     "testnull2.csv",
			fileName: "testnull2.csv",
			opts: NewReadOpts(
				InNeedNULL(true),
				InNULL(""),
			),
			args: args{
				[]interface{}{
					"", "", "",
				},
			},
			want: []interface{}{
				"2", nil, "g3",
			},
			wantErr: false,
		},
		{
			name:     "test_indefinite2.csv",
			fileName: "test_indefinite2.csv",
			opts:     NewReadOpts(),
			args: args{
				[]interface{}{
					"", "", "",
				},
			},
			want: []interface{}{
				"2", "Melon", nil,
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
			r, err := NewCSVReader(file, tt.opts)
			if err != nil {
				t.Error(err)
			}
			got, err := r.ReadRow(tt.args.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("CSVReader.ReadRow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CSVReader.ReadRow() = %v, want %v", got, tt.want)
			}
		})
	}
}
