package main

import (
	"io"
	"os"
)

// TRDSQL is output stream define
type TRDSQL struct {
	outStream io.Writer
	errStream io.Writer
	inSep     string
	ihead     bool
	iskip     int
	outHeader bool
	outSep    string
}

func main() {
	trdsql := &TRDSQL{outStream: os.Stdout, errStream: os.Stderr}
	os.Exit(trdsql.Run(os.Args))
}
