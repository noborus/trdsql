package main

import (
	"testing"
)

func TestLtsvInputNew(t *testing.T) {
	trdsql := trdsqlNew()
	file, err := tFileOpen("testdata/test.ltsv")
	if err != nil {
		t.Error(err)
	}
	_, err = trdsql.ltsvInputNew(file)
	if err != nil {
		t.Error(`ltsvInputNew error`)
	}
}
