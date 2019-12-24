package trdsql

import (
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
)

func TestCsvInputNew(t *testing.T) {
	file, err := singleFileOpen("testdata/test.csv")
	if err != nil {
		t.Error(err)
	}
	_, err = NewCSVReader(file, NewReadOpts())
	if err != nil {
		t.Error(`NewCSVReader error`)
	}
}

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
			name: "notEnouphHeader",
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

func TestCsvEmptyColumnRowNew(t *testing.T) {
	ro := NewReadOpts()
	ro.InHeader = true
	ro.InDelimiter = ","
	csvStream := `h1,h2
,v2`
	s := strings.NewReader(csvStream)
	r, err := NewCSVReader(s, ro)
	if err != nil {
		t.Error(err)
	}
	_, err = r.Names()
	if err != nil {
		t.Error(err)
	}
	record := make([]interface{}, 2)
	record, _ = r.ReadRow(record)
	if record[0] != "" || record[1] != "v2" {
		t.Errorf("invalid value [%s,%s]", record[0], record[1])
	}
}

func TestCsvColumnDifferenceNew(t *testing.T) {
	ro := NewReadOpts()
	ro.InHeader = true
	ro.InDelimiter = ","
	csvStream := `h1,h2,h3
	v1,v2,v3
	x1,x2
	z1`
	s := strings.NewReader(csvStream)
	r, _ := NewCSVReader(s, ro)
	_, err := r.Names()
	if err != nil {
		t.Error(err)
	}
	record := make([]interface{}, 3)
	for {
		record, err = r.ReadRow(record)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}
		if len(record) != 3 {
			t.Error("row difference")
		}
	}
}

func TestCsvIndefiniteInputFile(t *testing.T) {
	ro := NewReadOpts()
	ro.InHeader = false
	ro.InDelimiter = ","

	file, err := singleFileOpen("testdata/test_indefinite.csv")
	if err != nil {
		t.Error(err)
	}
	cr, err := NewCSVReader(file, ro)
	if err != nil {
		t.Error(`NewCSVReader error`)
	}
	list, err := cr.Names()
	if err != nil {
		t.Fatalf("Names error :%s", err)
	}
	if len(list) != 2 {
		t.Error(`invalid column`)
	}
}

func TestCsvIndefiniteInputFile2(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.csv")
	if err != nil {
		t.Error(err)
	}
	ro := NewReadOpts()
	ro.InHeader = false
	ro.InDelimiter = ","
	ro.InPreRead = 2
	cr, err := NewCSVReader(file, ro)
	if err != nil {
		t.Error(`NewCSVReader error`)
	}
	list, err := cr.Names()
	if err != nil {
		t.Fatalf("Names error :%s", err)
	}
	if len(list) != 3 {
		t.Error(`invalid column`)
	}
}

func TestCsvIndefiniteInputFile3(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.csv")
	if err != nil {
		t.Error(err)
	}
	ro := NewReadOpts()
	ro.InHeader = false
	ro.InDelimiter = ","
	ro.InPreRead = 100
	cr, err := NewCSVReader(file, ro)
	if err != nil {
		t.Error(`NewCSVReader error`)
	}
	list, err := cr.Names()
	if err != nil && err != io.EOF {
		t.Fatalf("Names error :%s", err)
	}
	if len(list) != 4 {
		t.Errorf("invalid column got = %d", len(list))
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
