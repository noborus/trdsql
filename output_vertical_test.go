package trdsql

import "testing"

func TestVfOutNew(t *testing.T) {
	out := NewVFWrite(NewWriteOpts())
	if out == nil {
		t.Error(`vfOut error`)
	}
}
