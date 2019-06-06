package trdsql

import "testing"

func TestRawOutNew(t *testing.T) {
	out := NewRAWWrite(",", false)
	if out == nil {
		t.Error(`rawOut error`)
	}
}
