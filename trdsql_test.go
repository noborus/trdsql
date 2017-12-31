package main

import (
	"bytes"
	"log"
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

func TestRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	for _, f := range outformat {
		for _, c := range tcsv {
			sql := "SELECT * FROM " + data + c[0]
			args := []string{"trdsql", f, sql}
			if trdsql.Run(args) != 0 {
				t.Errorf("trdsql error.")
			}
			t.Logf("%s\n%s\n", c[0], outStream.String())
			if outStream.String() == "" {
				t.Fatalf("trdsql error %s:%s:%s", c[0], c[1], trdsql.outStream)
			}
			outStream.Reset()
		}
	}
}

func TestCsvRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	for _, c := range tcsv {
		sql := "SELECT * FROM " + data + c[0]
		args := []string{"trdsql", sql}
		if trdsql.Run(args) != 0 {
			t.Errorf("trdsql error.")
		}
		if outStream.String() != c[1] {
			t.Fatalf("trdsql error %s:%s:%s", c[0], c[1], trdsql.outStream)
		}
		outStream.Reset()
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
		args := []string{"trdsql", "-iltsv", sql}
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
		args := []string{"trdsql", "-ijson", sql}
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
	args := []string{"trdsql", "-ig", sql}
	if trdsql.Run(args) != 0 {
		t.Errorf("trdsql error.")
	}
	if outStream.String() != "1,Orange,50\n2,Melon,500\n3,Apple,100\n" {
		t.Fatalf("trdsql error :%s", trdsql.outStream)
	}
	sql = "SELECT * FROM testdata/test.csv"
	args = []string{"trdsql", "-ig", sql}
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
		args := []string{"trdsql", "-q", "testdata/" + c}
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
	args := []string{"trdsql", "SELECT 1+1"}
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
	args := []string{"trdsql", "SELECT * FROM func()"}
	if trdsql.Run(args) == 0 {
		t.Errorf("trdsql error.")
	}
	if buf.String() == "" {
		t.Errorf("Should error.")
	}
}
