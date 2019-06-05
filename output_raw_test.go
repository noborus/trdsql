package trdsql

import "testing"

func TestRawOutNew(t *testing.T) {
	trdsql := trdsqlNew()
	out := trdsql.NewRAWWrite()
	if out == nil {
		t.Error(`rawOut error`)
	}
}
