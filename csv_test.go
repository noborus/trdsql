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

func TestStdinCsvOpen(t *testing.T) {
	_, error := csvOpen("-", ",", 0)
	if error != nil {
		t.Error(`Stdin csvOpen error`)
	}
}

func TestFileCsvOpen(t *testing.T) {
	f, _ := csvOpen("`not_file_dummy.csv`", ",", 0)
	if f != nil {
		t.Error(`not_file_dummy.csv Open error`)
	}
}
