package main

import "testing"

func TestRawOutNew(t *testing.T) {
	trdsql := trdsqlNew()
	out := trdsql.rawOutNew()
	if out == nil {
		t.Error(`rawOut error`)
	}
}
