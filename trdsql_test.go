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

func TestWildCard_Exec(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    string
		wantErr bool
	}{
		{
			name:    "testWildCardCSV",
			sql:     "SELECT * FROM " + dataDir + "tt*.csv",
			want:    "1,test1\n2,test2\n3,test3\n",
			wantErr: false,
		},
		{
			name:    "testWildCardLTSV",
			sql:     "SELECT id,name FROM " + dataDir + "tt*.ltsv",
			want:    "1,test1\n2,test2\n3,test3\n",
			wantErr: false,
		},
		{
			name:    "testWildCardJSON",
			sql:     "SELECT id,name FROM " + dataDir + "tt*.json",
			want:    "1,test1\n2,test2\n3,test3\n",
			wantErr: false,
		},
		{
			name:    "testWildCardTBLN",
			sql:     "SELECT id,name FROM " + dataDir + "tt*.tbln",
			want:    "1,test1\n2,test2\n3,test3\n",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		available := availableDB()
		for _, d := range available {
			t.Run(tt.name, func(t *testing.T) {
				outStream := new(bytes.Buffer)
				trd := setDefaultTRDSQL(outStream)
				trd.Driver = d[0]
				trd.Dsn = d[1]
				if err := trd.Exec(tt.sql); (err != nil) != tt.wantErr {
					t.Errorf("TRDSQL.Exec() error = %v, wantErr %v", err, tt.wantErr)
				}
				got := outStream.String()
				if got != tt.want {
					t.Errorf("TRDSQL.Exec() result = %v, want %v", got, tt.want)
				}
			})
		}
	}
}

func TestTRDSQL_Exec(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    string
		wantErr bool
	}{
		{
			name:    "test1",
			sql:     "SELECT 1",
			want:    "1\n",
			wantErr: false,
		},
		{
			name:    "testNoFile",
			sql:     "SELECT * testdata/notestfile.csv",
			want:    "",
			wantErr: true,
		},
		{
			name:    "testTestCSV",
			sql:     "SELECT * FROM " + dataDir + "test.csv",
			want:    "1,Orange\n2,Melon\n3,Apple\n",
			wantErr: false,
		},
		{
			name:    "testEscapedCSV",
			sql:     "SELECT * FROM \"" + dataDir + "test.csv\"",
			want:    "1,Orange\n2,Melon\n3,Apple\n",
			wantErr: false,
		},
		{
			name:    "testEscapedLTSV",
			sql:     "SELECT * FROM \"" + dataDir + "test.ltsv\"",
			want:    "1,Orange,50\n2,Melon,500\n3,Apple,100\n",
			wantErr: false,
		},
		{
			name:    "testJoin",
			sql:     "SELECT j.c1,j.c2,c.c1,c.c2 FROM " + dataDir + "test.json AS j LEFT JOIN " + dataDir + "test.csv AS c ON (j.c1 = c.c1)",
			want:    "1,Orange,1,Orange\n2,Melon,2,Melon\n3,Apple,3,Apple\n",
			wantErr: false,
		},
		{
			name:    "testSelfJoin",
			sql:     "SELECT * FROM " + dataDir + "test.csv AS t1 LEFT JOIN " + dataDir + "test.csv AS t2 ON (t1.c1 = t2.c1)",
			want:    "1,Orange,1,Orange\n2,Melon,2,Melon\n3,Apple,3,Apple\n",
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
			got := outStream.String()
			if got != tt.want {
				t.Errorf("TRDSQL.Exec() result = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTRDSQL_ExecOpts(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		opts    ReadOpt
		want    string
		wantErr bool
	}{
		{
			name:    "testSkipCSV",
			sql:     "SELECT * FROM " + dataDir + "test.csv",
			opts:    InSkip(1),
			want:    "2,Melon\n3,Apple\n",
			wantErr: false,
		},
		{
			name:    "testSkipLTSV",
			sql:     "SELECT * FROM " + dataDir + "test.ltsv",
			opts:    InSkip(1),
			want:    "2,Melon,500\n3,Apple,100\n",
			wantErr: false,
		},
		{
			name:    "testPreReadCSV",
			sql:     "SELECT * FROM " + dataDir + "test.csv",
			opts:    InPreRead(3),
			want:    "1,Orange\n2,Melon\n3,Apple\n",
			wantErr: false,
		},
		{
			name:    "testHeaderCSV",
			sql:     "SELECT id,name FROM " + dataDir + "header.csv",
			opts:    InHeader(true),
			want:    "1,Orange\n2,Melon\n3,Apple\n",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outStream := new(bytes.Buffer)
			exporter := NewExporter(NewWriter(OutStream(outStream)))
			importer := NewImporter(tt.opts)
			trd := NewTRDSQL(importer, exporter)
			if err := trd.Exec(tt.sql); (err != nil) != tt.wantErr {
				t.Errorf("TRDSQL.Exec() error = %v, wantErr %v", err, tt.wantErr)
			}
			got := outStream.String()
			if got != tt.want {
				t.Errorf("TRDSQL.Exec() result = %v, want %v", got, tt.want)
			}
		})
	}
}

func setDefaultTRDSQL(outStream io.Writer) *TRDSQL {
	importer := NewImporter(InFormat(GUESS))
	exporter := NewExporter(NewWriter(OutStream(outStream)))
	trd := NewTRDSQL(importer, exporter)
	trd.Driver = "sqlite3"
	trd.Dsn = ""
	return trd
}

func TestCSVRun(t *testing.T) {
	testCSV := [][]string{
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
	testLTSV := [][]string{
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
	testJSON := [][]string{
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
	testTBLN := [][]string{
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
	importer := NewImporter(
		InFormat(GUESS),
	)
	exporter := NewExporter(NewWriter(
		OutFormat(outFormat),
		OutStream(outStream),
	))
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
		{format: TBLN, result: "tbln"},
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
