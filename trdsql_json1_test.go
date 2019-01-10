// +build json1

package main

import (
	"bytes"
	"io"
	"testing"
)

func TestJSONFuncRun(t *testing.T) {
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trdsql := &TRDSQL{outStream: outStream, errStream: errStream}
	sql := "SELECT id,name,json_extract(attribute,'$.country') AS country, json_extract(attribute,'$.color') as color FROM testdata/test3.json"
	outstr := "1,Drolet,Maldives,burlywood\n2,Shelly,Yemen,plum\n3,Tuck,Mayotte,antiquewhite\n"
	args := []string{"trdsql", "-driver", "sqlite3", "-ijson", sql}
	if trdsql.Run(args) != 0 {
		t.Errorf("trdsql error.")
	}
	if outStream.String() != outstr {
		t.Fatalf("trdsql error \n[%s]\n[%s]\n", outstr, trdsql.outStream)
	}
}

func TestJSONIndefiniteInputFile(t *testing.T) {
	trdsql := trdsqlNew()
	file, err := tableFileOpen("testdata/test_indefinite.json")
	if err != nil {
		t.Error(err)
	}
	var jr Input
	jr, err = trdsql.jsonInputNew(file)
	if err != nil {
		t.Error(`csvInputNew error`)
	}
	list, err := jr.GetColumn(1)
	if err != nil {
		t.Fatalf("GetColumn error :%s", err)
	}
	if len(list) != 2 {
		t.Error(`invalid column`)
	}

}

func TestJSONIndefiniteInputFile2(t *testing.T) {
	trdsql := trdsqlNew()
	file, err := tableFileOpen("testdata/test_indefinite.json")
	if err != nil {
		t.Error(err)
	}
	var jr Input
	jr, err = trdsql.jsonInputNew(file)
	if err != nil {
		t.Error(`csvInputNew error`)
	}
	list, err := jr.GetColumn(2)
	if err != nil {
		t.Fatalf("GetColumn error :%s", err)
	}
	if len(list) != 3 {
		t.Error(`invalid column`)
	}
}

func TestJSONIndefiniteInputFile3(t *testing.T) {
	trdsql := trdsqlNew()
	file, err := tableFileOpen("testdata/test_indefinite.json")
	if err != nil {
		t.Error(err)
	}
	var jr Input
	jr, err = trdsql.jsonInputNew(file)
	if err != nil {
		t.Error(`csvInputNew error`)
	}
	list, err := jr.GetColumn(100)
	if err != nil && err != io.EOF {
		t.Fatalf("GetColumn error :%s", err)
	}
	if len(list) != 4 {
		t.Error(`invalid column`)
	}

}
