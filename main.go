package main

import (
	"io"
	"os"
)

// TRDSQL is output stream define
type TRDSQL struct {
	outStream, errStream io.Writer
}

func main() {
	trdsql := &TRDSQL{outStream: os.Stdout, errStream: os.Stderr}
	os.Exit(trdsql.Run(os.Args))
}
