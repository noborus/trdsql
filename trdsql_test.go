package trdsql

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	dataDir = "testdata"
)

func TestWildCard_Exec(t *testing.T) {
	tests := []struct {
		name     string
		sqlQuery string
		want     string
		wantErr  bool
	}{
		{
			name:     "testWildCardCSV",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "tt*.csv"),
			want:     "1,test1\n2,test2\n3,test3\n",
			wantErr:  false,
		},
		{
			name:     "testWildCardCS*",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test3.cs*"),
			want:     "1,Orange\n2,Melon\n3,Apple\n",
			wantErr:  false,
		},
		{
			name:     "testWildCardLTSV",
			sqlQuery: "SELECT id,name FROM " + filepath.Join(dataDir, "tt*.ltsv"),
			want:     "1,test1\n2,test2\n3,test3\n",
			wantErr:  false,
		},
		{
			name:     "testWildCardJSON",
			sqlQuery: "SELECT id,name FROM " + filepath.Join(dataDir, "tt*.json"),
			want:     "1,test1\n2,test2\n3,test3\n",
			wantErr:  false,
		},
		{
			name:     "testWildCardTBLN",
			sqlQuery: "SELECT id,name FROM " + filepath.Join(dataDir, "tt*.tbln"),
			want:     "1,test1\n2,test2\n3,test3\n",
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		available := availableDB()
		for _, d := range available {
			t.Run(tt.name, func(t *testing.T) {
				outStream := new(bytes.Buffer)
				ctx := context.Background()
				trd := setDefaultTRDSQL(outStream)
				trd.Driver = d[0]
				trd.Dsn = d[1]
				if err := trd.Exec(ctx, tt.sqlQuery); (err != nil) != tt.wantErr {
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
		name     string
		sqlQuery string
		want     string
		wantErr  bool
	}{
		{
			name:     "test1",
			sqlQuery: "SELECT 1",
			want:     "1\n",
			wantErr:  false,
		},
		{
			name:     "testNoFile",
			sqlQuery: "SELECT * testdata/notestfile.csv",
			want:     "",
			wantErr:  true,
		},
		{
			name:     "testTestCSV",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test.csv"),
			want:     "1,Orange\n2,Melon\n3,Apple\n",
			wantErr:  false,
		},
		{
			name:     "testTestCSV2",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "testcsv"),
			want:     "aaaaaaaa\nbbbbbbbb\ncccccccc\n",
			wantErr:  false,
		},
		{
			name:     "testQuoteCSV",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test_quote.csv"),
			want:     "1,\"O\"\"range\"\n2,M'elon\n3,A pple\n",
			wantErr:  false,
		},
		{
			name:     "testEscapedCSV",
			sqlQuery: "SELECT * FROM \"" + filepath.Join(dataDir, "test.csv\""),
			want:     "1,Orange\n2,Melon\n3,Apple\n",
			wantErr:  false,
		},
		{
			name:     "testEscapedLTSV",
			sqlQuery: "SELECT * FROM \"" + filepath.Join(dataDir, "test.ltsv\""),
			want:     "1,Orange,50\n2,Melon,500\n3,Apple,100\n",
			wantErr:  false,
		},
		{
			name:     "testTestJSON",
			sqlQuery: "SELECT c2 FROM \"" + filepath.Join(dataDir, "test.json\""),
			want:     "Orange\nMelon\nApple\n",
			wantErr:  false,
		},
		{
			name:     "testTestJSONJQ",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test.json::\".[] | .c2\""),
			want:     "Orange\nMelon\nApple\n",
			wantErr:  false,
		},
		{
			name:     "testTestYAML",
			sqlQuery: "SELECT c2 FROM \"" + filepath.Join(dataDir, "test.yaml\""),
			want:     "Orange\nMelon\nApple\n",
			wantErr:  false,
		},
		{
			name:     "testTestYAMLJQ",
			sqlQuery: "SELECT name,value FROM " + filepath.Join(dataDir, "sample.yaml::.b.e"),
			want:     "fred,3\nsam,4\n",
			wantErr:  false,
		},
		{
			name:     "testJoin",
			sqlQuery: "SELECT j.c1,j.c2,c.c1,c.c2 FROM " + filepath.Join(dataDir, "test.json") + " AS j LEFT JOIN " + filepath.Join(dataDir, "test.csv") + " AS c ON (j.c1 = c.c1)",
			want:     "1,Orange,1,Orange\n2,Melon,2,Melon\n3,Apple,3,Apple\n",
			wantErr:  false,
		},
		{
			name:     "testSelfJoin",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test.csv") + " AS t1 LEFT JOIN " + filepath.Join(dataDir, "test.csv") + " AS t2 ON (t1.c1 = t2.c1)",
			want:     "1,Orange,1,Orange\n2,Melon,2,Melon\n3,Apple,3,Apple\n",
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			outStream := new(bytes.Buffer)
			trd := setDefaultTRDSQL(outStream)
			if err := trd.Exec(ctx, tt.sqlQuery); (err != nil) != tt.wantErr {
				t.Errorf("TRDSQL.Exec() error = %v, wantErr %v", err, tt.wantErr)
			}
			got := outStream.String()
			if got != tt.want {
				t.Errorf("TRDSQL.Exec() result = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTRDSQL_Exec2(t *testing.T) {
	tests := []struct {
		name     string
		sqlQuery string
		want     string
		wantErr  bool
	}{
		{
			name:     "testTestCSV",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test.csv") + ";" + "SELECT * FROM " + filepath.Join(dataDir, "test.csv"),
			want:     "1,Orange\n2,Melon\n3,Apple\n1,Orange\n2,Melon\n3,Apple\n",
			wantErr:  false,
		},
		{
			name:     "UpdateTestCSV",
			sqlQuery: "UPDATE " + filepath.Join(dataDir, "test.csv") + " SET c2='Banana' WHERE c1='1';" + "SELECT * FROM " + filepath.Join(dataDir, "test.csv"),
			want:     "1,Banana\n2,Melon\n3,Apple\n",
			wantErr:  false,
		},
		{
			name:     "DeleteTestCSV",
			sqlQuery: "DELETE FROM " + filepath.Join(dataDir, "test.csv") + " WHERE c1='1'" + ";" + "SELECT * FROM " + filepath.Join(dataDir, "test.csv"),
			want:     "2,Melon\n3,Apple\n",
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			outStream := new(bytes.Buffer)
			trd := setDefaultTRDSQL(outStream)
			if err := trd.Exec(ctx, tt.sqlQuery); (err != nil) != tt.wantErr {
				t.Errorf("TRDSQL.Exec() error = %v, wantErr %v", err, tt.wantErr)
			}
			got := outStream.String()
			if got != tt.want {
				t.Errorf("TRDSQL.Exec() result = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTRDSQL_ErrExec(t *testing.T) {
	tests := []struct {
		name     string
		sqlQuery string
		want     string
		wantErr  bool
	}{
		{
			name:     "testNoFile",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "notestfile.csv"),
			want:     "",
			wantErr:  true,
		},
		{
			name:     "testNoColumn",
			sqlQuery: "SELECT test FROM " + filepath.Join(dataDir, "test.csv"),
			want:     "",
			wantErr:  true,
		},
		{
			name:     "testTypoSQL",
			sqlQuery: "ELECT * FROM " + filepath.Join(dataDir, "test.csv"),
			want:     "",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			outStream := new(bytes.Buffer)
			errStream := new(bytes.Buffer)
			importer := NewImporter(InFormat(GUESS))
			exporter := NewExporter(NewWriter(
				OutStream(outStream),
				ErrStream(errStream),
			),
			)
			trd := NewTRDSQL(importer, exporter)
			trd.Driver = DefaultDriver
			trd.Dsn = ""
			if err := trd.Exec(ctx, tt.sqlQuery); (err != nil) != tt.wantErr {
				t.Errorf("TRDSQL.Exec() error = %v, wantErr %v", err, tt.wantErr)
			}
			got := outStream.String()
			if got != tt.want {
				t.Errorf("TRDSQL.Exec() result = %v, want %v", got, tt.want)
			}
			errStr := errStream.String()
			if tt.wantErr && len(errStr) != 0 {
				t.Errorf("TRDSQL.Exec() returns with error but no message")
			}
		})
	}
}

func TestTRDSQL_ErrDBExec(t *testing.T) {
	tests := []struct {
		name    string
		driver  string
		dsn     string
		wantErr bool
	}{
		{
			name:    "testDB",
			driver:  "postgres",
			dsn:     "dbname=err",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			outStream := new(bytes.Buffer)
			errStream := new(bytes.Buffer)
			importer := NewImporter(InFormat(GUESS))
			exporter := NewExporter(NewWriter(
				OutStream(outStream),
				ErrStream(errStream),
			),
			)
			trd := NewTRDSQL(importer, exporter)
			trd.Driver = tt.driver
			trd.Dsn = tt.dsn
			if err := trd.Exec(ctx, "SELECT * FROM "+filepath.Join(dataDir, "test.csv")); (err != nil) != tt.wantErr {
				t.Errorf("TRDSQL.Exec() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTRDSQL_ErrWrite(t *testing.T) {
	tests := []struct {
		name     string
		sqlQuery string
		want     string
		wantErr  bool
	}{
		{
			name:     "testErrWrite",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test.csv"),
			want:     "",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			importer := NewImporter(InFormat(GUESS))
			exporter := NewExporter(errorWriter{})
			trd := NewTRDSQL(importer, exporter)
			trd.Driver = DefaultDriver
			trd.Dsn = ""
			if err := trd.Exec(ctx, tt.sqlQuery); (err != nil) != tt.wantErr {
				t.Errorf("TRDSQL.Exec() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTRDSQL_ReadOpts(t *testing.T) {
	tests := []struct {
		name     string
		sqlQuery string
		opts     ReadOpt
		want     string
		wantErr  bool
	}{
		{
			name:     "testSkipCSV",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test.csv"),
			opts:     InSkip(1),
			want:     "2,Melon\n3,Apple\n",
			wantErr:  false,
		},
		{
			name:     "testSkipLTSV",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test.ltsv"),
			opts:     InSkip(1),
			want:     "2,Melon,500\n3,Apple,100\n",
			wantErr:  false,
		},
		{
			name:     "testPreReadCSV",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test.csv"),
			opts:     InPreRead(3),
			want:     "1,Orange\n2,Melon\n3,Apple\n",
			wantErr:  false,
		},
		{
			name:     "testPreReadCSV2",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test.csv"),
			opts:     InPreRead(100),
			want:     "1,Orange\n2,Melon\n3,Apple\n",
			wantErr:  false,
		},
		{
			name:     "testHeaderCSV",
			sqlQuery: "SELECT id,name FROM " + filepath.Join(dataDir, "header.csv"),
			opts:     InHeader(true),
			want:     "1,Orange\n2,Melon\n3,Apple\n",
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			outStream := new(bytes.Buffer)
			exporter := NewExporter(NewWriter(OutStream(outStream)))
			importer := NewImporter(tt.opts)
			trd := NewTRDSQL(importer, exporter)
			if err := trd.Exec(ctx, tt.sqlQuery); (err != nil) != tt.wantErr {
				t.Errorf("TRDSQL.Exec() error = %v, wantErr %v", err, tt.wantErr)
			}
			got := outStream.String()
			if got != tt.want {
				t.Errorf("TRDSQL.Exec() result = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTRDSQL_WriteOpts(t *testing.T) {
	tests := []struct {
		name     string
		sqlQuery string
		opts     WriteOpt
		want     string
		wantErr  bool
	}{
		{
			name:     "testOutHeader",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test.csv"),
			opts:     OutHeader(true),
			want:     "c1,c2\n1,Orange\n2,Melon\n3,Apple\n",
			wantErr:  false,
		},
		{
			name:     "testOutAllQuotes",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test.csv"),
			opts:     OutAllQuotes(true),
			want:     "\"1\",\"Orange\"\n\"2\",\"Melon\"\n\"3\",\"Apple\"\n",
			wantErr:  false,
		},
		{
			name:     "testOutQuote1",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test_quote.csv"),
			opts:     OutQuote(`"`),
			want:     "1,\"O\"\"range\"\n2,M'elon\n3,A pple\n",
			wantErr:  false,
		},
		{
			name:     "testOutQuoteSingle",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test_quote.csv"),
			opts:     OutQuote(`'`),
			want:     "1,O\"range\n2,'M''elon'\n3,A pple\n",
			wantErr:  false,
		},
		{
			name:     "testOutQuote2",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test_quote2.csv"),
			opts:     OutUseCRLF(true),
			want:     "1,\"O\"\"range\"\r\n2,M'elon\r\n3,\"A pple\"\r\n4,\"ba\r\nnana\"\r\n",
			wantErr:  false,
		},
		{
			name:     "testOutCRLF",
			sqlQuery: "SELECT * FROM " + filepath.Join(dataDir, "test.csv"),
			opts:     OutUseCRLF(true),
			want:     "1,Orange\r\n2,Melon\r\n3,Apple\r\n",
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			outStream := new(bytes.Buffer)
			exporter := NewExporter(NewWriter(OutStream(outStream), tt.opts))
			importer := NewImporter()
			trd := NewTRDSQL(importer, exporter)
			if err := trd.Exec(ctx, tt.sqlQuery); (err != nil) != tt.wantErr {
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
	EnableMultipleQueries()
	importer := NewImporter(InFormat(GUESS))
	exporter := NewExporter(NewWriter(OutStream(outStream)))
	trd := NewTRDSQL(importer, exporter)
	trd.Driver = DefaultDriver
	trd.Dsn = ""
	return trd
}

func TestCSVRun(t *testing.T) {
	ctx := context.Background()
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
		sqlQuery := "SELECT * FROM " + filepath.Join(dataDir, c[0])
		err := trd.Exec(ctx, sqlQuery)
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
	ctx := context.Background()
	testLTSV := [][]string{
		{"test.ltsv", "1,Orange,50\n2,Melon,500\n3,Apple,100\n"},
		{"apache.ltsv", "[28/Feb/2013:12:00:00 +0900],192.168.0.1,GET /list HTTP/1.1,200,5316,-,Mozilla/5.0,9789,1,-,-,-\n[28/Feb/2013:12:00:00 +0900],172.16.0.12,GET /list HTTP/1.1,200,5316,-,Mozilla/5.0,9789,1,-,-,-\n"},
	}
	outStream := new(bytes.Buffer)
	trd := setDefaultTRDSQL(outStream)
	for _, c := range testLTSV {
		sqlQuery := "SELECT * FROM " + filepath.Join(dataDir, c[0])
		err := trd.Exec(ctx, sqlQuery)
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
	ctx := context.Background()
	testJSON := [][]string{
		{"test.json", "1,Orange\n2,Melon\n3,Apple\n"},
		{"test2.json", "1,Orange\n2,Melon\n3,Apple\n"},
	}
	outStream := new(bytes.Buffer)
	trd := setDefaultTRDSQL(outStream)
	for _, c := range testJSON {
		// The order of JSON import is undefined
		sqlQuery := "SELECT c1,c2 FROM " + filepath.Join(dataDir, c[0])
		err := trd.Exec(ctx, sqlQuery)
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
	ctx := context.Background()
	testTBLN := [][]string{
		{"test.tbln", "1,Bob\n2,Alice\n"},
		{"test2.tbln", "1,Orange\n2,Melon\n3,Apple\n"},
	}
	outStream := new(bytes.Buffer)
	trd := setDefaultTRDSQL(outStream)
	for _, c := range testTBLN {
		sqlQuery := "SELECT * FROM " + filepath.Join(dataDir, c[0])
		err := trd.Exec(ctx, sqlQuery)
		if err != nil {
			t.Errorf("trdsql error. %s", err)
		}
		if outStream.String() != c[1] {
			t.Fatalf("trdsql error %s:%s:%s", c[0], c[1], outStream)
		}
		outStream.Reset()
	}
}

func TestTextRun(t *testing.T) {
	ctx := context.Background()
	testText := [][]string{
		{"test.csv", `1,"1,Orange"
2,"2,Melon"
3,"3,Apple"
`},
		{"aiu.csv", "1,あ\n2,い\n3,う\n"},
	}
	outStream := new(bytes.Buffer)
	importer := NewImporter(
		InFormat(TEXT),
		InRowNumber(true),
	)
	exporter := NewExporter(NewWriter(OutStream(outStream)))
	trd := NewTRDSQL(importer, exporter)
	for _, c := range testText {
		sqlQuery := "SELECT * FROM " + filepath.Join(dataDir, c[0])
		err := trd.Exec(ctx, sqlQuery)
		if err != nil {
			t.Errorf("trdsql error %s", err)
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
	trd.Driver = DefaultDriver
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
		{format: JSONL, result: "jsonl"},
		{format: YAML, result: "yaml"},
	}
	sqlQuery := "SELECT * FROM " + filepath.Join(dataDir, "test.csv")
	for _, c := range testFormat {
		ctx := context.Background()
		outFormat := c.format
		outStream := new(bytes.Buffer)
		trd := setOutFormatTRDSQL(outFormat, outStream)
		err := trd.Exec(ctx, sqlQuery)
		if err != nil {
			t.Errorf("trdsql error. %s", err)
		}
		got := outStream.String()
		golden, err := os.ReadFile(filepath.Join("testdata", c.result+".golden"))
		if err != nil {
			t.Fatalf("failed reading .golden: %s", err)
		}
		want := string(golden)
		if got != want {
			t.Errorf("format: %s, got: %s, want: %s", c.format, got, want)
		}
		outStream.Reset()
	}
}

func pgDsn() string {
	pgDsn := os.Getenv("SESSION_PG_TEST_DSN")
	if pgDsn == "" {
		pgDsn = "dbname=trdsql_test"
	}
	return pgDsn
}

func myDsn() string {
	myDsn := os.Getenv("SESSION_MY_TEST_DSN")
	if myDsn == "" {
		myDsn = "root@/trdsql_test"
	}
	return myDsn
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
		{DefaultDriver, ""},
		{"postgres", pgDsn()},
		{"mysql", myDsn()},
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
			ctx := context.Background()
			available := availableDB()
			sqlQuery := "SELECT count(*) FROM " + filepath.Join(dataDir, tt.fileName)
			for _, d := range available {
				outStream := new(bytes.Buffer)
				trd := setDefaultTRDSQL(outStream)
				trd.Driver = d[0]
				trd.Dsn = d[1]
				if err := trd.Exec(ctx, sqlQuery); (err != nil) != tt.wantErr {
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

func TestFormat_String(t *testing.T) {
	tests := []struct {
		name string
		f    Format
		want string
	}{
		{
			name: "CSV",
			f:    CSV,
			want: "CSV",
		},
		{
			name: "LTSV",
			f:    LTSV,
			want: "LTSV",
		},
		{
			name: "JSON",
			f:    JSON,
			want: "JSON",
		},
		{
			name: "JSON",
			f:    JSON,
			want: "JSON",
		},
		{
			name: "TBLN",
			f:    TBLN,
			want: "TBLN",
		},
		{
			name: "RAW",
			f:    RAW,
			want: "RAW",
		},
		{
			name: "MD",
			f:    MD,
			want: "MD",
		},
		{
			name: "AT",
			f:    AT,
			want: "AT",
		},
		{
			name: "VF",
			f:    VF,
			want: "VF",
		},
		{
			name: "JSONL",
			f:    JSONL,
			want: "JSONL",
		},
		{
			name: "Unknown",
			f:    99,
			want: "Unknown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.f.String(); got != tt.want {
				t.Errorf("Format.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func benchmarkFormat(b *testing.B, format Format) {
	b.Helper()
	ctx := context.Background()
	sqlQuery := "SELECT * FROM " + filepath.Join(dataDir, "KEN_ALL.CSV")
	outStream := new(bytes.Buffer)
	importer := NewImporter(InFormat(GUESS))
	exporter := NewExporter(
		NewWriter(
			OutFormat(format),
			OutStream(outStream),
		),
	)
	trd := NewTRDSQL(importer, exporter)
	trd.Driver = DefaultDriver
	trd.Dsn = ""
	if err := trd.Exec(ctx, sqlQuery); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkOutput_CSV(b *testing.B) {
	benchmarkFormat(b, CSV)
}

func BenchmarkOutput_LTSV(b *testing.B) {
	benchmarkFormat(b, LTSV)
}

func BenchmarkOutput_RAW(b *testing.B) {
	benchmarkFormat(b, RAW)
}

func BenchmarkOutput_TBLN(b *testing.B) {
	benchmarkFormat(b, TBLN)
}

func BenchmarkOutput_JSON(b *testing.B) {
	benchmarkFormat(b, JSON)
}

func BenchmarkOutput_YAML(b *testing.B) {
	benchmarkFormat(b, YAML)
}
