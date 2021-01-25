// writer is an example of using a customized writer.
package main

import (
	"io"
	"log"
	"os"

	"github.com/noborus/trdsql"
)

type Write struct {
	writer io.Writer
}

func NewWriter(f io.Writer) *Write {
	return &Write{
		writer: f,
	}
}

func (w *Write) PreWrite(columns []string, types []string) error {
	return nil
}

func (w *Write) WriteRow(values []interface{}, columns []string) error {
	for i, v := range values {
		if i > 0 {
			if _, err := w.writer.Write([]byte(",")); err != nil {
				return err
			}
		}
		if _, err := w.writer.Write([]byte(trdsql.ValString(v))); err != nil {
			return err
		}
	}
	_, err := w.writer.Write([]byte("\n"))
	return err
}

func (w *Write) PostWrite() error {
	return nil
}

func main() {
	trd := trdsql.NewTRDSQL(
		trdsql.NewImporter(),
		trdsql.NewExporter(NewWriter(os.Stdout)),
	)
	err := trd.Exec("SELECT * FROM input.csv")
	if err != nil {
		log.Fatal(err)
	}
}
