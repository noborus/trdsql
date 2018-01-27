// +build json1

package main

import (
	"bytes"
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
