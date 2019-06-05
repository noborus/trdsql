package trdsql

import "testing"

func TestVfOutNew(t *testing.T) {
	trdsql := trdsqlNew()
	out := trdsql.NewVFWrite()
	if out == nil {
		t.Error(`vfOut error`)
	}
}
