package main

import (
	"bytes"
	"testing"
)

const (
	data = "testdata/"
)

func TestRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	csvq := &CSVQ{outStream: outStream, errStream: errStream}
	tcsv := []string{
		"test.csv",
		"testcsv",
	}
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
