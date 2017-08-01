package main

import (
	"strings"
	"testing"

	"github.com/najeira/ltsv"
)

func TestLtsvRead(t *testing.T) {
	const ltsvStream = `
ID:1	name:testa
ID:2	name:testb
	`
	s := strings.NewReader(ltsvStream)
	reader := ltsv.NewReader(s)
	r, _ := reader.Read()
	if r["ID"] != "1" || r["name"] != "testa" {
		t.Error("invalid value", r["ID"])
	}
}

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
