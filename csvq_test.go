package main

import (
	"bytes"
	"testing"
)

func TestRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	csvq := &CSVQ{outStream: outStream, errStream: errStream}
	args := []string{"csvq", "SELECT * FROM test.csv"}
	if csvq.Run(args) != 0 {
		t.Errorf("csvq error.")
	}
	if outStream.String() == "" {
		t.Fatalf("csvq error :%s", csvq.outStream)
	}
}
