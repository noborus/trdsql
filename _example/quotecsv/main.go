// quotecsv is an example of outputting a column quoted in double quotes.
// Customize the writer.
package main

import (
	"log"

	"github.com/noborus/trdsql"
)

func main() {
	trd := trdsql.NewTRDSQL(
		trdsql.NewImporter(),
		trdsql.NewExporter(
			trdsql.NewWriter(
				trdsql.OutAllQuotes(true),
			),
		),
	)
	err := trd.Exec("SELECT * FROM test.csv")
	if err != nil {
		log.Fatal(err)
	}
}
