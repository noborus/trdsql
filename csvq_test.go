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
}

func TestRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	csvq := &CSVQ{outStream: outStream, errStream: errStream}
	for _, c := range tcsv {
		sql := "SELECT * FROM testdata/" + c
		args := []string{"csvq", sql}
		if csvq.Run(args) != 0 {
			t.Errorf("csvq error.")
		}
		if outStream.String() == "" {
			t.Fatalf("csvq error :%s", csvq.outStream)
		}
	}
}

/*
func TestPgRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	csvq := &CSVQ{outStream: outStream, errStream: errStream}
	for _, c := range tcsv {
		sql := "SELECT * FROM testdata/" + c
		args := []string{"csvq", "-dbdriver", "postgres", sql}
		if csvq.Run(args) != 0 {
			t.Errorf("csvq error.")
		}
		if outStream.String() == "" {
			t.Fatalf("csvq error :%s", csvq.outStream)
		}
	}
}
*/
