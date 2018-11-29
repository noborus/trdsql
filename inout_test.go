package main

import (
	"os"
	"testing"
)

func TestValString(t *testing.T) {
	str := "test"
	if valString(str) != str {
		t.Errorf("valString error.")
	}
	if valString(nil) != "" {
		t.Errorf("valString error.")
	}
}

func TestSqlFields(t *testing.T) {
	str := "SELECT * FROM \"C:\\file with a space.csv\""
	w := sqlFields(str)
	if len(w) != 4 {
		t.Errorf("sqlFields error. element count:%d", len(w))
	}
	if w[len(w)-1] != "\"C:\\file with a space.csv\"" {
		t.Errorf("sqlFields error. element:%s", w[len(w)-1])
	}
}

func TestTableFileOpen(t *testing.T) {
	stdin, err := tableFileOpen("-")
	if err != nil && stdin == os.Stdin {
		t.Error(err)
	}
	f, err := tableFileOpen("inout_test.go")
	if err != nil {
		t.Error(err)
	}
	f.Close()

	// SQLite3 & MySQL escape
	f, err = tableFileOpen("`inout_test.go`")
	if err != nil {
		t.Error(err)
	}
	f.Close()

	// PostgreSQL escape
	f, err = tableFileOpen("\"inout_test.go\"")
	if err != nil {
		t.Error(err)
	}
	f.Close()
}
