package main

import (
	"io"
	"os"
)

// Input format
const (
	CSV = iota
	LTSV
	JSON
)

// TRDSQL is output stream define
type TRDSQL struct {
	outStream  io.Writer
	errStream  io.Writer
	driver     string
	dsn        string
	inSep      string
	inSkip     int
	inGuess    bool
	inType     int
	inHeader   bool
	inFirstRow bool
	outSep     string
	outHeader  bool
}

func main() {
	trdsql := &TRDSQL{outStream: os.Stdout, errStream: os.Stderr}
	os.Exit(trdsql.Run(os.Args))
}
