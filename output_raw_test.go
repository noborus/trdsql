package trdsql

import "testing"

func TestRawOutNew(t *testing.T) {
	out := NewRAWWriter(NewWriteOpts())
	if out == nil {
		t.Error(`rawOut error`)
	}
}
