package trdsql

import "testing"

func TestVfOutNew(t *testing.T) {
	out := NewVFWriter(NewWriteOpts())
	if out == nil {
		t.Error(`vfOut error`)
	}
}
