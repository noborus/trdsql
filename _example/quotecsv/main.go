// quotecsv is an example that outputs all columns quoted in double quotes.
// It customizes writer options.
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
