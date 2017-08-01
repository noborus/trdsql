package main

import (
	"bytes"
	"log"
	"testing"
)

const (
	data = "testdata/"
)

var tcsv = []string{
	"test.csv",
	"testcsv",
	"abc.csv",
	"aiu.csv",
	"hist.csv",
}

var outformat = []string{
	"",
	"-oltsv",
	"-oat",
	"-omd",
	"-ojson",
	"-oraw",
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
			sql := "SELECT * FROM testdata/" + c
			args := []string{"trdsql", f, sql}
			if trdsql.Run(args) != 0 {
				t.Errorf("trdsql error.")
			}
			t.Log(c, outStream.String())
			if outStream.String() == "" {
				t.Fatalf("trdsql error :%s:%s", c, trdsql.outStream)
			}
			outStream.Reset()
		}
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
		sql := "SELECT * FROM testdata/" + c
		args := []string{"trdsql", "-iltsv", sql}
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
	for _, c := range append(tcsv, tltsv...) {
		sql := "SELECT * FROM testdata/" + c
		args := []string{"trdsql", "-ig", sql}
		if trdsql.Run(args) != 0 {
			t.Errorf("trdsql error.")
		}
		if outStream.String() == "" {
			t.Fatalf("trdsql error :%s", trdsql.outStream)
		}
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
	if guessExtension("test.ltsv") != true {
		t.Errorf("guessExtension error.")
	}
	if guessExtension("test.csv") != false {
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
