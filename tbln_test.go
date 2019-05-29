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
	trdsql := trdsqlNew()
	tr, err := trdsql.tblnInputNew(s)
	if err != nil {
		t.Errorf(`tblnInputNew error: %s`, err)
	}
	list, err := tr.GetColumn(1)
	if err != nil {
		t.Errorf(`GetColumn error: %s`, err)
	}
	if len(list) == 0 {
		t.Error(`0 column`)
	}
}

func TestTblnFile(t *testing.T) {
	trdsql := trdsqlNew()
	file, err := tableFileOpen("testdata/test.tbln")
	want := [][]interface{}{{"1", "Bob"}}
	if err != nil {
		t.Error(err)
	}
	var tr Input
	tr, err = trdsql.tblnInputNew(file)
	if err != nil {
		t.Error(`tblnInputNew error`)
	}
	list, err := tr.GetColumn(1)
	if err != nil {
		t.Error(`GetColumn error`)
	}
	if len(list) != 2 {
		t.Error(`invalid column`)
	}
	got := tr.PreReadRow()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Tbln file %v, want %v", got, want)
	}
}
