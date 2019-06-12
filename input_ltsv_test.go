package trdsql

import (
	"io"
	"strings"
	"testing"
)

func TestLtsvInputNew(t *testing.T) {
	const ltsvStream = `ID:1	name:test`
	s := strings.NewReader(ltsvStream)
	lr, err := NewLTSVReader(s, NewReadOpts())
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.Names()
	if err != nil {
		t.Error(`Names error`)
	}
	if len(list) == 0 {
		t.Error(`0 column`)
	}
}

func TestLtsvInvalidInputNew(t *testing.T) {
	const ltsvStream = `ID;1	name:test`
	s := strings.NewReader(ltsvStream)
	ro := NewReadOpts()
	ro.InPreRead = 1
	lr, _ := NewLTSVReader(s, ro)
	_, err := lr.Names()
	if err != nil {
		if err.Error() != "invalid column" {
			t.Error(err)
		}
	}
}

func TestLtsvFile(t *testing.T) {
	file, err := singleFileOpen("testdata/test.ltsv")
	if err != nil {
		t.Error(err)
	}
	lr, err := NewLTSVReader(file, NewReadOpts())
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.Names()
	if err != nil {
		t.Error(`Names error`)
	}
	if len(list) != 3 {
		t.Error(`invalid column`)
	}
}

func TestIndefiniteLtsvFile1(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.ltsv")
	if err != nil {
		t.Error(err)
	}
	lr, err := NewLTSVReader(file, NewReadOpts())
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.Names()
	if err != nil {
		t.Error(`Names error`)
	}
	if len(list) != 3 {
		t.Error(`invalid column`)
	}
}

func TestIndefiniteLtsvFile2(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.ltsv")
	if err != nil {
		t.Error(err)
	}
	ro := NewReadOpts()
	ro.InPreRead = 2
	lr, err := NewLTSVReader(file, ro)
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.Names()
	if err != nil {
		t.Error(`Names error`)
	}
	if len(list) != 4 {
		t.Error(`invalid column`)
	}
}

func TestIndefiniteLtsvFile3(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.ltsv")
	if err != nil {
		t.Error(err)
	}
	ro := NewReadOpts()
	ro.InPreRead = 100
	lr, err := NewLTSVReader(file, ro)
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.Names()
	if err != nil && err != io.EOF {
		t.Error(err)
	}
	if len(list) != 5 {
		t.Error(`invalid column`)
	}
}
