package trdsql

import (
	"testing"
)

func TestValString(t *testing.T) {
	str := "test"
	if valString(str) != str {
		t.Errorf("valString error.")
	}
	if valString(nil) != "" {
		t.Errorf("valString error.")
	}
}

