package trdsql

import (
	"bytes"
	"io"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	dataDir = "testdata/"
)

func TestTRDSQL_Exec(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    []byte
		wantErr bool
	}{
		{
			name:    "test1",
			sql:     "SELECT 1",
			want:    []byte("1\n"),
			wantErr: false,
		},
		{
			name:    "testTestCSV",
			sql:     "SELECT * FROM " + dataDir + "test.csv",
			want:    []byte("1,Orange\n2,Melon\n3,Apple\n"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outStream := new(bytes.Buffer)
			trd := setDefaultTRDSQL(outStream)
			if err := trd.Exec(tt.sql); (err != nil) != tt.wantErr {
				t.Errorf("TRDSQL.Exec() error = %v, wantErr %v", err, tt.wantErr)
			}
			got := outStream.Bytes()
			if !bytes.Equal(got, tt.want) {
				t.Errorf("TRDSQL.Exec() result = %v, want %v", got, tt.want)
			}
		})
	}
}

func setDefaultTRDSQL(outStream io.Writer) *TRDSQL {
	readOpts := NewReadOpts()
	readOpts.InFormat = GUESS
	writeOpts := NewWriteOpts()
	writeOpts.OutStream = outStream
	importer := NewImporter(readOpts)
	exporter := NewExporter(writeOpts, NewWriter(writeOpts))
	trd := NewTRDSQL(importer, exporter)
	trd.Driver = "sqlite3"
	trd.Dsn = ""
	return trd
}

func TestCSVRun(t *testing.T) {
	var testCSV = [][]string{
		{"test.csv", "1,Orange\n2,Melon\n3,Apple\n"},
		{"testcsv", "aaaaaaaa\nbbbbbbbb\ncccccccc\n"},
		{"abc.csv", "a1\na2\n"},
		{"aiu.csv", "あ\nい\nう\n"},
		{"hist.csv", "1,2017-7-10\n2,2017-7-10\n2,2017-7-11\n"},
		{"test.csv.gz", "1,Orange\n2,Melon\n3,Apple\n"},
	}
	outStream := new(bytes.Buffer)
	trd := setDefaultTRDSQL(outStream)
	for _, c := range testCSV {
		sql := "SELECT * FROM " + dataDir + c[0]
		err := trd.Exec(sql)
		if err != nil {
			t.Errorf("trdsql error. %s", err)
		}
		if outStream.String() != c[1] {
			t.Fatalf("trdsql error %s:%s:%s", c[0], c[1], outStream)
		}
		outStream.Reset()
	}
}

func TestLTSVRun(t *testing.T) {
	var testLTSV = [][]string{
		{"test.ltsv", "1,Orange,50\n2,Melon,500\n3,Apple,100\n"},
		{"apache.ltsv", "[28/Feb/2013:12:00:00 +0900],192.168.0.1,GET /list HTTP/1.1,200,5316,-,Mozilla/5.0,9789,1,-,-,-\n[28/Feb/2013:12:00:00 +0900],172.16.0.12,GET /list HTTP/1.1,200,5316,-,Mozilla/5.0,9789,1,-,-,-\n"},
	}
	outStream := new(bytes.Buffer)
	trd := setDefaultTRDSQL(outStream)
	for _, c := range testLTSV {
		sql := "SELECT * FROM " + dataDir + c[0]
		err := trd.Exec(sql)
		if err != nil {
			t.Errorf("trdsql error. %s", err)
		}
		if outStream.String() != c[1] {
			t.Fatalf("trdsql error %s:%s:%s", c[0], c[1], outStream)
		}
		outStream.Reset()
	}
}

func TestJSONRun(t *testing.T) {
	var testJSON = [][]string{
		{"test.json", "1,Orange\n2,Melon\n3,Apple\n"},
		{"test2.json", "1,Orange\n2,Melon\n3,Apple\n"},
	}
	outStream := new(bytes.Buffer)
	trd := setDefaultTRDSQL(outStream)
	for _, c := range testJSON {
		// The order of JSON import is undefined
		sql := "SELECT c1,c2 FROM " + dataDir + c[0]
		err := trd.Exec(sql)
		if err != nil {
			t.Errorf("trdsql error. %s", err)
		}
		if outStream.String() != c[1] {
			t.Fatalf("trdsql error %s:%s:%s", c[0], c[1], outStream)
		}
		outStream.Reset()
	}
}

func TestTBLNRun(t *testing.T) {
	var testTBLN = [][]string{
		{"test.tbln", "1,Bob\n2,Alice\n"},
		{"test2.tbln", "1,Orange\n2,Melon\n3,Apple\n"},
	}
	outStream := new(bytes.Buffer)
	trd := setDefaultTRDSQL(outStream)
	for _, c := range testTBLN {
		sql := "SELECT * FROM " + dataDir + c[0]
		err := trd.Exec(sql)
		if err != nil {
			t.Errorf("trdsql error. %s", err)
		}
		if outStream.String() != c[1] {
			t.Fatalf("trdsql error %s:%s:%s", c[0], c[1], outStream)
		}
		outStream.Reset()
	}
}

func setOutFormatTRDSQL(outFormat Format, outStream io.Writer) *TRDSQL {
	readOpts := NewReadOpts()
	readOpts.InFormat = GUESS
	writeOpts := NewWriteOpts()
	writeOpts.OutFormat = outFormat
	writeOpts.OutStream = outStream
	importer := NewImporter(readOpts)
	exporter := NewExporter(writeOpts, NewWriter(writeOpts))
	trd := NewTRDSQL(importer, exporter)
	trd.Driver = "sqlite3"
	trd.Dsn = ""
	return trd
}

func TestOutFormatRun(t *testing.T) {
	type form struct {
		format Format
		result string
	}
	testFormat := []form{
		{format: CSV, result: "csv"},
		{format: JSON, result: "json"},
		{format: LTSV, result: "ltsv"},
		{format: VF, result: "vf"},
		{format: RAW, result: "raw"},
		{format: MD, result: "md"},
		{format: AT, result: "at"},
	}
	sql := "SELECT * FROM " + dataDir + "test.csv"
	for _, c := range testFormat {
		outFormat := c.format
		outStream := new(bytes.Buffer)
		trd := setOutFormatTRDSQL(outFormat, outStream)
		err := trd.Exec(sql)
		if err != nil {
			t.Errorf("trdsql error. %s", err)
		}
		g, err := ioutil.ReadFile(filepath.Join("testdata", c.result+".golden"))
		if err != nil {
			t.Fatalf("failed reading .golden: %s", err)
		}
		if !bytes.Equal(outStream.Bytes(), g) {
			t.Fatalf("trdsql error %s:%s:%s", g, c.format, outStream)
		}
		outStream.Reset()
	}
}

func checkDBTest(driver string, dsn string) bool {
	db, err := Connect(driver, dsn)
	if err != nil {
		return false
	}
	err = db.Ping()
	if err != nil {
		return false
	}
	err = db.Close()
	return (err == nil)
}
func availableDB() [][]string {
	database := [][]string{
		{"sqlite3", ""},
		{"postgres", "dbname=trdsql_test"},
		{"mysql", "root@/trdsql_test"},
	}
	available := make([][]string, 0)
	for _, d := range database {
		if checkDBTest(d[0], d[1]) {
			available = append(available, []string{d[0], d[1]})
		}
	}
	return available
}

func TestTRDSQL_FileExec(t *testing.T) {
	tests := []struct {
		fileName string
		want     int
		wantErr  bool
	}{
		{fileName: "test1.csv", want: 3, wantErr: false},
		{fileName: "KEN_ALL.CSV", want: 124165, wantErr: false},
		{fileName: "abc.csv", want: 2, wantErr: false},
		{fileName: "aiu.csv", want: 3, wantErr: false},
		{fileName: "apache.ltsv", want: 2, wantErr: false},
		{fileName: "hist.csv", want: 3, wantErr: false},
		{fileName: "test.csv.gz", want: 3, wantErr: false},
		{fileName: "test.json", want: 3, wantErr: false},
		{fileName: "test.ltsv", want: 3, wantErr: false},
		{fileName: "test.tbln", want: 2, wantErr: false},
		{fileName: "test2.csv", want: 3, wantErr: false},
		{fileName: "test2.json", want: 3, wantErr: false},
		{fileName: "test2.tbln", want: 3, wantErr: false},
		{fileName: "test3.csv", want: 3, wantErr: false},
		{fileName: "test3.json", want: 3, wantErr: false},
		{fileName: "test_indefinite.csv", want: 3, wantErr: false},
		{fileName: "test_indefinite.json", want: 3, wantErr: false},
		{fileName: "test_indefinite.ltsv", want: 3, wantErr: false},
		{fileName: "testcsv", want: 3, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.fileName, func(t *testing.T) {
			available := availableDB()
			sql := "SELECT count(*) FROM " + dataDir + tt.fileName
			for _, d := range available {
				outStream := new(bytes.Buffer)
				trd := setDefaultTRDSQL(outStream)
				trd.Driver = d[0]
				trd.Dsn = d[1]
				if err := trd.Exec(sql); (err != nil) != tt.wantErr {
					t.Errorf("TRDSQL.Exec() error = %v, wantErr %v", err, tt.wantErr)
				}
				got := outStream.String()
				result, err := strconv.Atoi(strings.TrimRight(got, "\n"))
				if err != nil {
					t.Fatal(err)
				}
				if result != tt.want {
					t.Errorf("TRDSQL.Exec() result = %v, want %v", got, tt.want)
				}
			}
		})
	}
}
