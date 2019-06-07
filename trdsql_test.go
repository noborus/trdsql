package trdsql

import (
	"bytes"
)

const (
	data = "testdata/"
)

var TCSV = [][]string{
	{"test.csv", "1,Orange\n2,Melon\n3,Apple\n"},
	{"testcsv", "aaaaaaaa\nbbbbbbbb\ncccccccc\n"},
	{"abc.csv", "a1\na2\n"},
	{"aiu.csv", "あ\nい\nう\n"},
	{"hist.csv", "1,2017-7-10\n2,2017-7-10\n2,2017-7-11\n"},
}

var outformat = []string{
	"",
	"-oltsv",
	"-oat",
	"-omd",
	"-ojson",
	"-oraw",
	"-ovf",
	"-otbln",
}

func trdsqlNew() *TRDSQL {
	trd := NewTRDSQL(&Import, &Export)
	outStream, errStream := new(bytes.Buffer), new(bytes.Buffer)
	trd.WriteOpts.OutStream = outStream
	trd.WriteOpts.ErrStream = errStream
	return trd
}
