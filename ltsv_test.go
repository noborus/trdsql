package main

import (
	"strings"
	"testing"
)

func TestLtsvInputNew(t *testing.T) {
	const ltsvStream = `ID:1	name:test`
	s := strings.NewReader(ltsvStream)
	trdsql := trdsqlNew()
	lr, err := trdsql.ltsvInputNew(s)
	if err != nil {
		t.Error(`ltsvInputNew error`)
	}
	list, err := lr.firstRead()
	if err != nil {
		t.Error(`firstRead error`)
	}
	if len(list) == 0 {
		t.Error(`0 column`)
	}
}

func TestLtsvInvalidInputNew(t *testing.T) {
	const ltsvStream = `ID;1	name:test`
	s := strings.NewReader(ltsvStream)
	trdsql := trdsqlNew()
	lr, _ := trdsql.ltsvInputNew(s)
	_, err := lr.firstRead()
	if err.Error() != "LTSV format error" {
		t.Error()
	}
}

func TestLtsvFile(t *testing.T) {
	trdsql := trdsqlNew()
	file, err := tableFileOpen("testdata/test.ltsv")
	if err != nil {
		t.Error(err)
	}
	var lr Input
	lr, err = trdsql.ltsvInputNew(file)
	if err != nil {
		t.Error(`ltsvInputNew error`)
	}
	list, err := lr.firstRead()
	if err != nil {
		t.Error(`firstRead error`)
	}
	if len(list) == 0 {
		t.Error(`0 column`)
	}
}
