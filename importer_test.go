package trdsql

import (
	"io"
	"os"
	"reflect"
	"testing"
)

func TestNewImporter(t *testing.T) {
	type args struct {
		inputFormat Format
	}
	tests := []struct {
		name string
		args args
		want Format
	}{
		{
			name: "test1",
			args: args{inputFormat: CSV},
			want: CSV,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readOpts := NewReadOpts()
			readOpts.InFormat = CSV
			if got := NewImporter(readOpts); !reflect.DeepEqual(got.ReadOpts.InFormat, tt.want) {
				t.Errorf("NewImporter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_listTable(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "noTable",
			query: "SELECT 1;",
			want:  nil,
		},
		{
			name:  "testTable",
			query: "SELECT * FROM test;",
			want:  []string{"test"},
		},
		{
			name:  "testJoin",
			query: "SELECT test.a FROM test LEFT JOIN test2 ON (test.b = test2.b);",
			want:  []string{"test", "test2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := listTable(tt.query); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("listTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestImportFile(t *testing.T) {
	type args struct {
		db       *DB
		fileName string
		opts     ReadOpts
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ImportFile(tt.args.db, tt.args.fileName, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ImportFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ImportFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_guessExtension(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		want      Format
	}{
		{name: "testCSV", tableName: "test.csv", want: CSV},
		{name: "testLTSV", tableName: "test.ltsv", want: LTSV},
		{name: "testJSON", tableName: "test.json", want: JSON},
		{name: "testTBLN", tableName: "test.tbln", want: TBLN},
		{name: "testunknown", tableName: "test.go", want: CSV},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := guessExtension(tt.tableName); got != tt.want {
				t.Errorf("guessExtension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_importFileOpen(t *testing.T) {
	type args struct {
		tableName string
	}
	tests := []struct {
		name    string
		args    args
		want    io.ReadCloser
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := importFileOpen(tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("importFileOpen() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("importFileOpen() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_tableFileOpen(t *testing.T) {
	type args struct {
		fileName string
	}
	tests := []struct {
		name    string
		args    args
		want    io.ReadCloser
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tableFileOpen(tt.args.fileName)
			if (err != nil) != tt.wantErr {
				t.Errorf("tableFileOpen() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("tableFileOpen() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_globFileOpen(t *testing.T) {
	type args struct {
		globName string
	}
	tests := []struct {
		name    string
		args    args
		want    *io.PipeReader
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := globFileOpen(tt.args.globName)
			if (err != nil) != tt.wantErr {
				t.Errorf("globFileOpen() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("globFileOpen() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_trimQuote(t *testing.T) {
	type args struct {
		fileName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := trimQuote(tt.args.fileName); got != tt.want {
				t.Errorf("trimQuote() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_extFileReader(t *testing.T) {
	type args struct {
		fileName string
		reader   *os.File
	}
	tests := []struct {
		name string
		args args
		want io.ReadCloser
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extFileReader(tt.args.fileName, tt.args.reader); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("extFileReader() = %v, want %v", got, tt.want)
			}
		})
	}
}
