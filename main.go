package main

import (
	"io"
	"os"
)

// CSVQ is output stream define
type CSVQ struct {
	outStream, errStream io.Writer
}

func main() {
	csvq := &CSVQ{outStream: os.Stdout, errStream: os.Stderr}
	os.Exit(csvq.Run(os.Args))
}
