// slice is to import data using NewSliceImporter.
package main

import (
	"fmt"
	"log"

	"github.com/noborus/trdsql"
)

func sliceQuery(slice any, tableName string, query string) ([][]any, error) {
	// trdsql.EnableDebug()
	importer := trdsql.NewSliceImporter(tableName, slice)
	writer := trdsql.NewSliceWriter()
	trd := trdsql.NewTRDSQL(importer, trdsql.NewExporter(writer))
	err := trd.Exec(query)
	return writer.Table, err
}

func main() {
	data := []struct {
		id   int
		name string
	}{
		{id: 1, name: "Bod"},
		{id: 2, name: "Alice"},
		{id: 3, name: "Henry"},
	}
	table, err := sliceQuery(data, "slice", "SELECT name,id FROM slice ORDER BY id DESC")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(table)
	// Output:
	// [[Henry 3] [Alice 2] [Bod 1]]
}
