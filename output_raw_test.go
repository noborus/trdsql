package trdsql

import "testing"

func TestRawOutNew(t *testing.T) {
	out := NewRAWWrite(NewWriteOpts())
	if out == nil {
		t.Error(`rawOut error`)
	}
}
