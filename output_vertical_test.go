package main

import "testing"

func TestVfOutNew(t *testing.T) {
	trdsql := trdsqlNew()
	out := trdsql.vfOutNew()
	if out == nil {
		t.Error(`vfOut error`)
	}
}
