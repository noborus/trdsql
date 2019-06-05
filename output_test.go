package trdsql

import (
	"testing"
)

func TestValString(t *testing.T) {
	str := "test"
	if ValString(str) != str {
		t.Errorf("ValString error.")
	}
	if ValString(nil) != "" {
		t.Errorf("ValString error.")
	}
}

