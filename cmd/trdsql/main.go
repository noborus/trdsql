package main

import (
	"os"

	"github.com/noborus/trdsql/cmd"
)

func main() {
	cli := cmd.Cli{
		OutStream: os.Stdout,
		ErrStream: os.Stderr,
	}
	os.Exit(cli.Run(os.Args))
}
