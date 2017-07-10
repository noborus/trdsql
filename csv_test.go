package main

import (
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
