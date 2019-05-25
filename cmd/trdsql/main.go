package main

import (
	"github.com/noborus/trdsql"

	"os"
)

func main() {
	tr := &trdsql.TRDSQL{OutStream: os.Stdout, ErrStream: os.Stderr}

	os.Exit(tr.Run(os.Args))
}
