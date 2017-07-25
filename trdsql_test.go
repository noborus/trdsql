package main

import (
	"bytes"
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
			if outStream.String() == "" {
				t.Fatalf("trdsql error :%s", trdsql.outStream)
			}
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

/*
func TestPgRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	for _, c := range tcsv {
		sql := "SELECT * FROM testdata/" + c
		args := []string{"trdsql", "-driver", "postgres", sql}
		if trdsql.Run(args) != 0 {
			t.Errorf("trdsql error.")
		}
		if outStream.String() == "" {
			t.Fatalf("trdsql error :%s", trdsql.outStream)
		}
	}
}
*/
