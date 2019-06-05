// +build json1

package trdsql

import (
	"io"
	"testing"
)


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
