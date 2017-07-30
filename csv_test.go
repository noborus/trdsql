package main

import (
	"testing"
)

func IsSeparator(s string) bool {
	r, _ := getSeparator(s)
	if r > 0 {
		return true
	}
	return false
}
func IsNotSeparator() bool {
	r, _ := getSeparator("false")
	if r == ',' {
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

func TestCsvInputNew(t *testing.T) {
	trdsql := trdsqlNew()
	file, err := tFileOpen("testdata/test.csv")
	if err != nil {
		t.Error(err)
	}
	_, err = trdsql.csvInputNew(file)
	if err != nil {
		t.Error(`csvInputNew error`)
	}
}

func TestNoCsvInputNew(t *testing.T) {
	trdsql := trdsqlNew()
	file, err := tFileOpen("nofile")
	if err == nil {
		t.Error(`Should error`)
	}
	_, err = trdsql.csvInputNew(file)
	if err != nil {
		t.Error(`csvInputNew error`)
	}
}

func TestCsvOutNew(t *testing.T) {
	trdsql := trdsqlNew()
	out := trdsql.csvOutNew()
	if out == nil {
		t.Error(`csvOut error`)
	}
}

func TestCsvOutNewFalse(t *testing.T) {
	trdsql := trdsqlNew()
	trdsql.outSep = "**"
	out := trdsql.csvOutNew()
	if out == nil {
		t.Error(`csvOut error`)
	}
}
