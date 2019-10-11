package trdsql

import (
	"io"
	"strings"
	"testing"
)

func IsDelimiter(s string) bool {
	r, _ := delimiter(s)
	return (r > 0)
}

func IsNotDelimiter() bool {
	r, _ := delimiter("false")
	return (r == ',')
}

func TestGetDelimiter(t *testing.T) {
	if !IsDelimiter(",") {
		t.Error(`IsDelimiter(",") = false`)
	}
	if !IsNotDelimiter() {
		t.Error(`IsNotDelimiter() = false`)
	}
}

func TestCsvInputNew(t *testing.T) {
	file, err := singleFileOpen("testdata/test.csv")
	if err != nil {
		t.Error(err)
	}
	_, err = NewCSVReader(file, NewReadOpts())
	if err != nil {
		t.Error(`NewCSVReader error`)
	}
}

func TestCsvEmptyNew(t *testing.T) {
	const csvStream = ``
	s := strings.NewReader(csvStream)
	r, err := NewCSVReader(s, NewReadOpts())
	if err != nil {
		t.Error(err)
	}
	_, err = r.Names()
	if err == nil {
		t.Error(`csvEmpty is should error`)
	}
}

func TestCsvHeaderNew(t *testing.T) {
	ro := NewReadOpts()
	ro.InHeader = true
	ro.InDelimiter = ","
	ro.InPreRead = 1
	csvStream := `he1,he2
v1,v2
`
	s := strings.NewReader(csvStream)
	r, _ := NewCSVReader(s, ro)
	header, err := r.Names()
	if err != nil {
		t.Error(err)
	}
	if header[0] != "he1" || header[1] != "he2" {
		t.Error("invalid header")
	}
}

func TestCsvEmptyColumnHeaderNew(t *testing.T) {
	ro := NewReadOpts()
	ro.InHeader = true
	ro.InDelimiter = ","
	csvStream := `h1,
	v1,v2`
	s := strings.NewReader(csvStream)
	r, _ := NewCSVReader(s, ro)
	header, _ := r.Names()
	if header[0] != "h1" || header[1] != "c2" {
		t.Error("invalid header")
	}
}

func TestCsvEmptyColumnRowNew(t *testing.T) {
	ro := NewReadOpts()
	ro.InHeader = true
	ro.InDelimiter = ","
	csvStream := `h1,h2
,v2`
	s := strings.NewReader(csvStream)
	r, err := NewCSVReader(s, ro)
	if err != nil {
		t.Error(err)
	}
	_, err = r.Names()
	if err != nil {
		t.Error(err)
	}
	record := make([]interface{}, 2)
	record, _ = r.ReadRow(record)
	if record[0] != "" || record[1] != "v2" {
		t.Errorf("invalid value [%s,%s]", record[0], record[1])
	}
}

func TestCsvColumnDifferenceNew(t *testing.T) {
	ro := NewReadOpts()
	ro.InHeader = true
	ro.InDelimiter = ","
	csvStream := `h1,h2,h3
	v1,v2,v3
	x1,x2
	z1`
	s := strings.NewReader(csvStream)
	r, _ := NewCSVReader(s, ro)
	_, err := r.Names()
	if err != nil {
		t.Error(err)
	}
	record := make([]interface{}, 3)
	for {
		record, err = r.ReadRow(record)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}
		if len(record) != 3 {
			t.Error("row difference")
		}
	}
}

func TestCsvIndefiniteInputFile(t *testing.T) {
	ro := NewReadOpts()
	ro.InHeader = false
	ro.InDelimiter = ","

	file, err := singleFileOpen("testdata/test_indefinite.csv")
	if err != nil {
		t.Error(err)
	}
	cr, err := NewCSVReader(file, ro)
	if err != nil {
		t.Error(`NewCSVReader error`)
	}
	list, err := cr.Names()
	if err != nil {
		t.Fatalf("Names error :%s", err)
	}
	if len(list) != 2 {
		t.Error(`invalid column`)
	}
}

func TestCsvIndefiniteInputFile2(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.csv")
	if err != nil {
		t.Error(err)
	}
	ro := NewReadOpts()
	ro.InHeader = false
	ro.InDelimiter = ","
	ro.InPreRead = 2
	cr, err := NewCSVReader(file, ro)
	if err != nil {
		t.Error(`NewCSVReader error`)
	}
	list, err := cr.Names()
	if err != nil {
		t.Fatalf("Names error :%s", err)
	}
	if len(list) != 3 {
		t.Error(`invalid column`)
	}
}

func TestCsvIndefiniteInputFile3(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.csv")
	if err != nil {
		t.Error(err)
	}
	ro := NewReadOpts()
	ro.InHeader = false
	ro.InDelimiter = ","
	ro.InPreRead = 100
	cr, err := NewCSVReader(file, ro)
	if err != nil {
		t.Error(`NewCSVReader error`)
	}
	list, err := cr.Names()
	if err != nil && err != io.EOF {
		t.Fatalf("Names error :%s", err)
	}
	if len(list) != 4 {
		t.Errorf("invalid column got = %d", len(list))
	}
}
