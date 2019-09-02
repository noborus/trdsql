// +build json1

package trdsql

import (
	"io"
	"testing"
)

func TestJSONIndefiniteInputFile(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.json")
	if err != nil {
		t.Error(err)
	}
	jr, err := NewJSONReader(file, NewReadOpts())
	if err != nil {
		t.Error(`csvInputNew error`)
	}
	list, err := jr.Names()
	if err != nil {
		t.Fatalf("Names error :%s", err)
	}
	if len(list) != 2 {
		t.Error(`invalid column`)
	}
}

func TestJSONIndefiniteInputFile2(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.json")
	if err != nil {
		t.Error(err)
	}
	ro := NewReadOpts()
	ro.InPreRead = 2
	jr, err := NewJSONReader(file, ro)
	if err != nil {
		t.Error(`csvInputNew error`)
	}
	list, err := jr.Names()
	if err != nil {
		t.Fatalf("Names error :%s", err)
	}
	if len(list) != 3 {
		t.Error(`invalid column`)
	}
}

func TestJSONIndefiniteInputFile3(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.json")
	if err != nil {
		t.Error(err)
	}
	ro := NewReadOpts()
	ro.InPreRead = 100
	jr, err := NewJSONReader(file, ro)
	if err != nil {
		t.Errorf("NewJSONReader error: %s", err)
	}
	list, err := jr.Names()
	if err != nil && err != io.EOF {
		t.Fatalf("Names error :%s", err)
	}
	if len(list) != 4 {
		t.Error(`invalid column`)
	}
}
