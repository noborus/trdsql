package trdsql

import (
	"reflect"
	"strings"
	"testing"
)

func TestTblnInputNew(t *testing.T) {
	const tblnStream = `; name: | id | name |
| 1 | test |`
	s := strings.NewReader(tblnStream)
	tr, err := NewTBLNReader(s)
	if err != nil {
		t.Errorf(`tblnInputNew error: %s`, err)
	}
	list, err := tr.Names()
	if err != nil {
		t.Errorf(`Names error: %s`, err)
	}
	if len(list) == 0 {
		t.Error(`0 column`)
	}
}

func TestTblnFile(t *testing.T) {
	file, err := singleFileOpen("testdata/test.tbln")
	want := [][]interface{}{{"1", "Bob"}}
	if err != nil {
		t.Error(err)
	}
	tr, err := NewTBLNReader(file)
	if err != nil {
		t.Error(`tblnInputNew error`)
	}
	list, err := tr.Names()
	if err != nil {
		t.Error(`Names error`)
	}
	if len(list) != 2 {
		t.Error(`invalid column`)
	}
	got := tr.PreReadRow()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Tbln file %v, want %v", got, want)
	}
}
