package main

import (
	"io"
	"strings"
	"testing"
)

func IsSeparator(s string) bool {
	r, _ := separator(s)
	return (r > 0)
}

func IsNotSeparator() bool {
	r, _ := separator("false")
	return (r == ',')
}

func TestGetSeparator(t *testing.T) {
	if !IsSeparator(",") {
		t.Error(`IsSeparator(",") = false`)
	}
	if !IsNotSeparator() {
		t.Error(`IsNotSeparator() = false`)
	}
}

func TestCsvInputNew(t *testing.T) {
	trdsql := trdsqlNew()
	file, err := tableFileOpen("testdata/test.csv")
	if err != nil {
		t.Error(err)
	}
	_, err = trdsql.csvInputNew(file)
	if err != nil {
		t.Error(`csvInputNew error`)
	}
}

func TestCsvEmptyNew(t *testing.T) {
	trdsql := trdsqlNew()
	trdsql.inSep = ","
	const csvStream = ``
	s := strings.NewReader(csvStream)
	r, err := trdsql.csvInputNew(s)
	if err != nil {
		t.Error(err)
	}
	_, err = r.FirstRead()
	if err.Error() != "EOF" {
		t.Error(err)
	}
}

func TestCsvHeaderNew(t *testing.T) {
	trdsql := trdsqlNew()
	trdsql.inHeader = true
	trdsql.inSep = ","
	csvStream := `h1,h2
	v1,v2`
	s := strings.NewReader(csvStream)
	r, _ := trdsql.csvInputNew(s)
	header, _ := r.FirstRead()
	if header[0] != "h1" || header[1] != "h2" {
		t.Error("invalid header")
	}
}

func TestCsvEmptyColumnHeaderNew(t *testing.T) {
	trdsql := trdsqlNew()
	trdsql.inHeader = true
	trdsql.inSep = ","
	csvStream := `h1,
	v1,v2`
	s := strings.NewReader(csvStream)
	r, _ := trdsql.csvInputNew(s)
	header, _ := r.FirstRead()
	if header[0] != "h1" || header[1] != "c2" {
		t.Error("invalid header")
	}
}

func TestCsvEmptyColumnRowNew(t *testing.T) {
	trdsql := trdsqlNew()
	trdsql.inHeader = true
	trdsql.inSep = ","
	csvStream := `h1,h2
	,v2`
	s := strings.NewReader(csvStream)
	r, _ := trdsql.csvInputNew(s)
	_, err := r.FirstRead()
	if err != nil {
		t.Error(err)
	}
	record := make([]interface{}, 2)
	record, _ = r.RowRead(record)
	if record[0] != "" || record[1] != "v2" {
		t.Error("invalid value")
	}
}

func TestCsvColumnDifferenceNew(t *testing.T) {
	trdsql := trdsqlNew()
	trdsql.inHeader = true
	trdsql.inSep = ","
	csvStream := `h1,h2,h3
	v1,v2,v3
	x1,x2
	z1`
	s := strings.NewReader(csvStream)
	r, _ := trdsql.csvInputNew(s)
	_, err := r.FirstRead()
	if err != nil {
		t.Error(err)
	}
	record := make([]interface{}, 3)
	for {
		record, err = r.RowRead(record)
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

func TestCsvNoInputNew(t *testing.T) {
	trdsql := trdsqlNew()
	file, err := tableFileOpen("nofile")
	if err == nil {
		t.Error(`Should error`)
	}
	_, err = trdsql.csvInputNew(file)
	if err != nil {
		t.Error(`csvInputNew error`)
	}
}

func TestCsvOutNew(t *testing.T) {
	trdsql := trdsqlNew()
	out := trdsql.csvOutNew()
	if out == nil {
		t.Error(`csvOut error`)
	}
}
