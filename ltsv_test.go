package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/najeira/ltsv"
)

func TestStdinLtsvOpen(t *testing.T) {
	_, error := ltsvOpen("-", "\t", 0)
	if error != nil {
		t.Error(`Stdin csvOpen error`)
	}
}

func TestFileLtsvOpen(t *testing.T) {
	f, _ := ltsvOpen("`not_file_dummy.ltsv`", "\t", 0)
	if f != nil {
		t.Error(`not_file_dummy.cltsv Open error`)
	}
}

func TestLtsvRead(t *testing.T) {
	const ltsvStream = `
ID:1	name:testa
ID:2	name:testb
	`
	s := strings.NewReader(ltsvStream)
	reader := ltsv.NewReader(s)
	r, _ := reader.Read()
	if r["ID"] != "1" || r["name"] != "testa" {
		fmt.Printf("[%s]\n", r["ID"])
		t.Error("invalid value", r["ID"])
	}
}
