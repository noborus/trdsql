package trdsql

import (
	"bytes"
	"io"
	"log"
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
		want     [][]any
	}{
		{
			name:     "testBlank",
			fileName: "testnull.csv",
			opts:     NewReadOpts(),
			want: [][]any{
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
			want: [][]any{
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
			want: [][]any{
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
			name:     "test.csv",
			fileName: "test.csv",
			opts:     NewReadOpts(),
			args: args{
				[]any{
					"", "",
				},
			},
			want: []any{
				"2", "Melon",
			},
			wantErr: false,
		},
		{
			name:     "testblank",
			fileName: "testnull2.csv",
			opts:     NewReadOpts(),
			args: args{
				[]any{
					"", "", "",
				},
			},
			want: []any{
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
				[]any{
					"", "", "",
				},
			},
			want: []any{
				"2", nil, "g3",
			},
			wantErr: false,
		},
		{
			name:     "test_indefinite2.csv",
			fileName: "test_indefinite2.csv",
			opts:     NewReadOpts(),
			args: args{
				[]any{
					"", "", "",
				},
			},
			want: []any{
				"2", "Melon", nil,
			},
			wantErr: false,
		},
		{
			name:     "test_indefinite.csv truncation",
			fileName: "test_indefinite.csv",
			opts:     NewReadOpts(),
			args: args{
				[]any{
					"", "",
				},
			},
			want: []any{
				"2", "Melon",
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

func TestCSVReader_ReadRow_TruncationWarning(t *testing.T) {
	origDebug := debug
	defer func() { debug = origDebug }()

	tests := []struct {
		name        string
		csvData     string
		opts        *ReadOpts
		row         []any
		enableDebug bool
		wantRow     []any
		wantWarn    bool
		warnSnippet string
	}{
		{
			name:        "truncated row with debug enabled logs warning",
			csvData:     "id,name\n2,beta,extraA,extraB\n",
			opts:        NewReadOpts(InHeader(true)),
			row:         make([]any, 2),
			enableDebug: true,
			wantRow:     []any{"2", "beta"},
			wantWarn:    true,
			warnSnippet: "row has 4 columns, but table schema has 2 columns (extra columns truncated)",
		},
		{
			name:        "truncated row with debug disabled logs nothing",
			csvData:     "id,name\n2,beta,extraA,extraB\n",
			opts:        NewReadOpts(InHeader(true)),
			row:         make([]any, 2),
			enableDebug: false,
			wantRow:     []any{"2", "beta"},
			wantWarn:    false,
		},
		{
			name:        "matching columns with debug enabled logs no warning",
			csvData:     "id,name\n2,beta\n",
			opts:        NewReadOpts(InHeader(true)),
			row:         make([]any, 2),
			enableDebug: true,
			wantRow:     []any{"2", "beta"},
			wantWarn:    false,
		},
		{
			name:        "fewer columns with debug enabled logs no warning and fills nil",
			csvData:     "id,name,extra\n2,beta\n",
			opts:        NewReadOpts(InHeader(true)),
			row:         make([]any, 3),
			enableDebug: true,
			wantRow:     []any{"2", "beta", nil},
			wantWarn:    false,
		},
		{
			name:        "empty row slice for skipRead logs no warning",
			csvData:     "1,2,3,4\n5,6,7,8\n",
			opts:        NewReadOpts(),
			row:         nil,
			enableDebug: true,
			wantRow:     nil,
			wantWarn:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			debug = debugT(tt.enableDebug)
			var buf bytes.Buffer
			origOutput := log.Writer()
			origFlags := log.Flags()
			log.SetOutput(&buf)
			log.SetFlags(0)
			defer func() {
				log.SetOutput(origOutput)
				log.SetFlags(origFlags)
			}()

			r, err := NewCSVReader(strings.NewReader(tt.csvData), tt.opts)
			if err != nil {
				t.Fatalf("unexpected NewCSVReader error: %v", err)
			}

			got, err := r.ReadRow(tt.row)
			if err != nil {
				t.Fatalf("unexpected ReadRow error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.wantRow) {
				t.Errorf("ReadRow() = %v, want %v", got, tt.wantRow)
			}

			logged := buf.String()
			if tt.wantWarn {
				if !strings.Contains(logged, tt.warnSnippet) {
					t.Errorf("expected warning snippet %q in log output, got %q", tt.warnSnippet, logged)
				}
			} else {
				if strings.Contains(logged, "extra columns truncated") {
					t.Errorf("expected no truncation warning, got %q", logged)
				}
			}
		})
	}
}

func TestCSVReader_ReadRow_PreReadParity(t *testing.T) {
	csvData := "1,Orange\n2,Melon,ibaraki\n3,Apple,aomori,red\n"

	// With InPreRead = 1, row 2 and 3 are read via ReadRow with schema width 2 (truncated).
	r1, err := NewCSVReader(strings.NewReader(csvData), NewReadOpts(InPreRead(1)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	names1, _ := r1.Names()
	if len(names1) != 2 {
		t.Errorf("expected 2 columns with InPreRead(1), got %d", len(names1))
	}
	row1 := make([]any, len(names1))
	got1, err := r1.ReadRow(row1)
	if err != nil {
		t.Fatalf("unexpected ReadRow error: %v", err)
	}
	if !reflect.DeepEqual(got1, []any{"2", "Melon"}) {
		t.Errorf("expected truncated row %v, got %v", []any{"2", "Melon"}, got1)
	}

	// With InPreRead = 3, all rows are pre-read and expand the schema to 4 columns.
	r3, err := NewCSVReader(strings.NewReader(csvData), NewReadOpts(InPreRead(3)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	names3, _ := r3.Names()
	if len(names3) != 4 {
		t.Errorf("expected 4 columns with InPreRead(3), got %d", len(names3))
	}
	preRows := r3.PreReadRow()
	if len(preRows) != 3 {
		t.Fatalf("expected 3 pre-read rows, got %d", len(preRows))
	}
	if !reflect.DeepEqual(preRows[1], []any{"2", "Melon", "ibaraki", nil}) {
		t.Errorf("expected expanded pre-read row %v, got %v", []any{"2", "Melon", "ibaraki", nil}, preRows[1])
	}
	if !reflect.DeepEqual(preRows[2], []any{"3", "Apple", "aomori", "red"}) {
		t.Errorf("expected expanded pre-read row %v, got %v", []any{"3", "Apple", "aomori", "red"}, preRows[2])
	}
}
