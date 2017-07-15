package main

import (
	"encoding/csv"
	"strings"
	"testing"
)

func IsSeparator(s string) bool {
	if getSeparator(s) > 0 {
		return true
	}
	return false
}
func IsNotSeparator() bool {
	if getSeparator("false") == ',' {
		return true
	}
	return false
}

func TestGetSeparator(t *testing.T) {
	if !IsSeparator(",") {
		t.Error(`IsSeparator(",") = false`)
	}
	if !IsNotSeparator() {
		t.Error(`IsNotSeparator() = false`)
	}
}

func TestStdinCsvOpen(t *testing.T) {
	_, error := csvOpen("-")
	if error != nil {
		t.Error(`Stdin csvOpen error`)
	}
}

func TestFileCsvOpen(t *testing.T) {
	f, _ := csvOpen("`not_file_dummy.csv`")
	if f != nil {
		t.Error(`not_file_dummy.csv Open error`)
	}
}

func TestCsvRead(t *testing.T) {
	const csvStream = `
	a,b,c
	1,2,3
	`
	s := strings.NewReader(csvStream)
	c := headerRead(csv.NewReader(s))
	if c[0] != "a" {

	}
}
