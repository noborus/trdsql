package main

import (
	"fmt"
	"log"

	"github.com/noborus/trdsql"
)

type ArrayTable struct {
	table [][]string
}

func (a *ArrayTable) PreWrite(columns []string, types []string) error {
	a.table = make([][]string, 0)
	fmt.Println(columns, types)
	return nil
}
func (a *ArrayTable) WriteRow(values []interface{}, columns []string) error {
	row := make([]string, len(values))
	for i, v := range values {
		row[i] = trdsql.ValString(v)
	}
	a.table = append(a.table, row)
	return nil
}
func (a *ArrayTable) PostWrite() error {
	return nil
}

func exec(query string) [][]string {
	writer := &ArrayTable{}
	trd := trdsql.NewTRDSQL(trdsql.NewImporter(), trdsql.NewExporter(writer))
	trd.Driver = "postgres"
	trd.Dsn = ""
	err := trd.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
	return writer.table
}

func main() {
	trdsql.EnableDebug()
	table := exec("SELECT * FROM test")
	fmt.Println(table)
}
