package trdsql

import (
	"io"
	"strings"
	"testing"
)

func TestLtsvInputNew(t *testing.T) {
	const ltsvStream = `ID:1	name:test`
	s := strings.NewReader(ltsvStream)
	lr, err := NewLTSVReader(s)
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.GetColumn(1)
	if err != nil {
		t.Error(`GetColumn error`)
	}
	if len(list) == 0 {
		t.Error(`0 column`)
	}
}

func TestLtsvInvalidInputNew(t *testing.T) {
	const ltsvStream = `ID;1	name:test`
	s := strings.NewReader(ltsvStream)
	lr, _ := NewLTSVReader(s)
	_, err := lr.GetColumn(1)
	if err.Error() != "LTSV format error" {
		t.Error()
	}
}

func TestLtsvFile(t *testing.T) {
	file, err := tableFileOpen("testdata/test.ltsv")
	if err != nil {
		t.Error(err)
	}
	lr, err := NewLTSVReader(file)
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.GetColumn(1)
	if err != nil {
		t.Error(`GetColumn error`)
	}
	if len(list) != 3 {
		t.Error(`invalid column`)
	}
}

func TestIndefiniteLtsvFile1(t *testing.T) {
	file, err := tableFileOpen("testdata/test_indefinite.ltsv")
	if err != nil {
		t.Error(err)
	}
	lr, err := NewLTSVReader(file)
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.GetColumn(1)
	if err != nil {
		t.Error(`GetColumn error`)
	}
	if len(list) != 3 {
		t.Error(`invalid column`)
	}
}

func TestIndefiniteLtsvFile2(t *testing.T) {
	file, err := tableFileOpen("testdata/test_indefinite.ltsv")
	if err != nil {
		t.Error(err)
	}
	lr, err := NewLTSVReader(file)
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.GetColumn(2)
	if err != nil {
		t.Error(`GetColumn error`)
	}
	if len(list) != 4 {
		t.Error(`invalid column`)
	}
}

func TestIndefiniteLtsvFile3(t *testing.T) {
	file, err := tableFileOpen("testdata/test_indefinite.ltsv")
	if err != nil {
		t.Error(err)
	}
	lr, err := NewLTSVReader(file)
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.GetColumn(100)
	if err != nil && err != io.EOF {
		t.Error(`GetColumn error`)
	}
	if len(list) != 5 {
		t.Error(`invalid column`)
	}
}
