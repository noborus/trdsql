package trdsql

import (
	"os"
	"testing"
)

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
	stdin, err := singleFileOpen("-")
	if err != nil && stdin == os.Stdin {
		t.Error(err)
	}
	f, err := singleFileOpen("input_test.go")
	if err != nil {
		t.Error(err)
	}
	f.Close()

	// SQLite3 & MySQL escape
	f, err = singleFileOpen("`input_test.go`")
	if err != nil {
		t.Error(err)
	}
	f.Close()

	// PostgreSQL escape
	f, err = singleFileOpen("\"input_test.go\"")
	if err != nil {
		t.Error(err)
	}
	f.Close()
}
