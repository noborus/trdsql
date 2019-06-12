package main

import (
	"os"

	"github.com/noborus/trdsql/cmd"
)

func main() {
	os.Exit(cmd.Run(os.Args))
}
