// quotecsv is an example of outputting a column quoted in double quotes.
// Customize the writer.
package main

import (
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/noborus/trdsql"
)

type QuoteWrite struct {
	writer io.Writer
}

func NewQuoteCSVWriter(w io.Writer) *QuoteWrite {
	return &QuoteWrite{
		writer: w,
	}
}

func (w *QuoteWrite) PreWrite(columns []string, types []string) error {
	return nil
}

func (w *QuoteWrite) WriteRow(values []interface{}, columns []string) error {
	qColumns := make([]string, len(values))
	for i, col := range values {
		qColumns[i] = strconv.Quote(trdsql.ValString(col))
	}
	_, err := w.writer.Write([]byte(strings.Join(qColumns, ",") + "\n"))
	return err
}

func (w *QuoteWrite) PostWrite() error {
	return nil
}

func main() {
	trd := trdsql.NewTRDSQL(
		trdsql.NewImporter(),
		trdsql.NewExporter(NewQuoteCSVWriter(os.Stdout)),
	)
	err := trd.Exec("SELECT * FROM testdata/test.csv")
	if err != nil {
		log.Fatal(err)
	}
}
