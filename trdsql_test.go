package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
)

const (
	data = "testdata/"
)

var tcsv = [][]string{
	{"test.csv", "1,Orange\n2,Melon\n3,Apple\n"},
	{"testcsv", "aaaaaaaa\nbbbbbbbb\ncccccccc\n"},
	{"abc.csv", "a1\na2\n"},
	{"aiu.csv", "あ\nい\nう\n"},
	{"hist.csv", "1,2017-7-10\n2,2017-7-10\n2,2017-7-11\n"},
}

var outformat = []string{
	"",
	"-oltsv",
	"-oat",
	"-omd",
	"-ojson",
	"-oraw",
	"-ovf",
}

func trdsqlNew() *TRDSQL {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	return trdsql
}

func TestCsvRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	for _, c := range tcsv {
		sql := "SELECT * FROM " + data + c[0]
		args := []string{"trdsql", "-driver", "sqlite3", sql}
		if trdsql.Run(args) != 0 {
			t.Errorf("trdsql error.")
		}
		if outStream.String() != c[1] {
			t.Fatalf("trdsql error %s:%s:%s", c[0], c[1], trdsql.outStream)
		}
		outStream.Reset()
	}
}

func TestCsvHeaderRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	sql := "SELECT * FROM " + data + "header.csv"
	outstr := "1,Orange\n2,Melon\n3,Apple\n"
	args := []string{"trdsql", "-driver", "sqlite3", "-ih", sql}
	if trdsql.Run(args) != 0 {
		t.Errorf("trdsql error.")
	}
	if outStream.String() != outstr {
		t.Fatalf("trdsql error %s:%s:%s", "header.csv", outstr, trdsql.outStream)
	}
	outStream.Reset()
}

func TestOutHeaderRun(t *testing.T) {
	outstr := "c1,c2\n1,Orange\n2,Melon\n3,Apple\n"
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	sql := "SELECT * FROM " + data + "test.csv"
	args := []string{"trdsql", "-driver", "sqlite3", "-oh", sql}
	if trdsql.Run(args) != 0 {
		t.Errorf("trdsql error.")
	}
	if outStream.String() != outstr {
		t.Fatalf("trdsql error %s:%s:%s", "test.csv", outstr, trdsql.outStream)
	}
}

var tltsv = []string{
	"test.ltsv",
	"apache.ltsv",
}

func TestLtsvRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	for _, c := range tltsv {
		sql := "SELECT * FROM " + data + c
		args := []string{"trdsql", "-driver", "sqlite3", "-iltsv", sql}
		if trdsql.Run(args) != 0 {
			t.Errorf("trdsql error.")
		}
		if outStream.String() == "" {
			t.Fatalf("trdsql error :%s", trdsql.outStream)
		}
	}
}

var tjson = []string{
	"test.json",
	"test2.json",
}

func TestJSONRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	for _, c := range tjson {
		sql := "SELECT * FROM " + data + c
		args := []string{"trdsql", "-driver", "sqlite3", "-ijson", sql}
		if trdsql.Run(args) != 0 {
			t.Errorf("trdsql error.")
		}
		if outStream.String() == "" {
			t.Fatalf("trdsql error :%s", trdsql.outStream)
		}
	}
}

func TestGuessRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	sql := "SELECT id,name,price FROM testdata/test.ltsv"
	args := []string{"trdsql", "-driver", "sqlite3", "-ig", sql}
	if trdsql.Run(args) != 0 {
		t.Errorf("trdsql error.")
	}
	if outStream.String() != "1,Orange,50\n2,Melon,500\n3,Apple,100\n" {
		t.Fatalf("trdsql error :%s", trdsql.outStream)
	}
	sql = "SELECT * FROM testdata/test.csv"
	args = []string{"trdsql", "-driver", "sqlite3", "-ig", sql}
	if trdsql.Run(args) != 0 {
		t.Errorf("trdsql error.")
	}
	outs := outStream.String()
	if outs[0] != '1' {
		t.Fatalf("trdsql error %s:%s", outs, trdsql.outStream)
	}
}

var tsql = []string{
	"test.sql",
}

func TestQueryfileRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	for _, c := range tsql {
		args := []string{"trdsql", "-driver", "sqlite3", "-q", "testdata/" + c}
		if trdsql.Run(args) != 0 {
			t.Errorf("trdsql error.")
		}
		if outStream.String() == "" {
			t.Fatalf("trdsql error :%s", trdsql.outStream)
		}
	}
}

func TestGuessExtension(t *testing.T) {
	if guessExtension("test.ltsv") != LTSV {
		t.Errorf("guessExtension error.")
	}
	if guessExtension("test.json") != JSON {
		t.Errorf("guessExtension error.")
	}
	if guessExtension("test.csv") != CSV {
		t.Errorf("guessExtension error.")
	}
}

func TestNoFrom(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	args := []string{"trdsql", "-driver", "sqlite3", "SELECT 1+1"}
	if trdsql.Run(args) != 0 {
		t.Errorf("trdsql error.")
	}
	if outStream.String() != "2\n" {
		t.Fatalf("trdsql error :%s", trdsql.outStream)
	}
}

func TestFromFunc(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	args := []string{"trdsql", "-driver", "sqlite3", "SELECT * FROM func()"}
	if trdsql.Run(args) == 0 {
		t.Errorf("trdsql error.")
	}
	if buf.String() == "" {
		t.Errorf("Should error.")
	}
}

var tdsn = map[string]string{
	"sqlite3":  "",
	"postgres": "dbname=trdsql_test",
	"mysql":    "root:@/trdsql_test",
}

var tdb = map[string]bool{
	"sqlite3":  true,
	"postgres": true,
	"mysql":    true,
}

func TestDbRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	for db, dbc := range tdb {
		if !dbc {
			continue
		}
		for _, f := range outformat {
			for _, c := range tcsv {
				sql := "SELECT * FROM " + data + c[0]
				args := []string{"trdsql", "-driver", db, "-dsn", tdsn[db], f, sql}
				if trdsql.Run(args) != 0 {
					t.Errorf("trdsql error.")
				}
				if outStream.String() == "" {
					t.Fatalf("trdsql error %s:%s:%s", c[0], c[1], trdsql.outStream)
				}
				outStream.Reset()
			}
		}
	}
}

func TestCountKENALLRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	log.SetOutput(errStream)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	csv := "KEN_ALL.CSV"
	count := "124165"
	for db, dbc := range tdb {
		if !dbc {
			continue
		}
		sql := "SELECT count(*) FROM " + data + csv
		args := []string{"trdsql", "-driver", db, "-dsn", tdsn[db], sql}
		if trdsql.Run(args) != 0 {
			t.Errorf("%s\n%s", db, errStream.String())
		}
		outStr := strings.TrimRight(outStream.String(), "\n")
		if outStr != count {
			t.Fatalf("%s:%s:%s", csv, count, outStr)
		}
		outStream.Reset()
	}
}

func dbcheck(d string) bool {
	db, err := Connect(d, tdsn[d])
	if err != nil {
		log.Printf("%s:%s\n", d, err)
		return false
	}
	defer db.Disconnect()
	_, err = db.Exec("SELECT 1")
	if err != nil {
		log.Printf("%s:%s\n", d, err)
		return false
	}
	return true
}

func setup() {
	if !dbcheck("postgres") {
		tdb["postgres"] = false
		fmt.Println("PostgreSQL could not connect, skipping")
	}
	if !dbcheck("mysql") {
		tdb["mysql"] = false
		fmt.Println("MySQL could not connect, skipping")
	}
}

func teardown() {
}

func TestMain(m *testing.M) {
	setup()
	ret := m.Run()
	if ret == 0 {
		teardown()
	}
	os.Exit(ret)
}
