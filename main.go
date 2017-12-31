package main

import (
	"io"
	"os"
)

// TRDSQL is output stream define
type TRDSQL struct {
	outStream io.Writer
	errStream io.Writer
	driver    string
	dsn       string
	inSep     string
	outSep    string
	iskip     int
	iguess    bool
	iltsv     bool
	ijson     bool
	ihead     bool
	ifrow     bool
	outHeader bool
	omd       bool
}

func main() {
	trdsql := &TRDSQL{outStream: os.Stdout, errStream: os.Stderr}
	os.Exit(trdsql.Run(os.Args))
}
