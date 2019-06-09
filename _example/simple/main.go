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

func main() {
	trdsql.EnableDebug()

	readOpts := trdsql.NewReadOpts()
	readOpts.InFormat = trdsql.CSV

	writeOpts := trdsql.NewWriteOpts()
	writer := &ArrayTable{}
	// trd := trdsql.NewTRDSQL(trdsql.NewImporter(readOpts), trdsql.NewExporter(writeOpts, writer))
	trd := trdsql.NewTRDSQL(nil, trdsql.NewExporter(writeOpts, writer))
	trd.Driver = "postgres"
	trd.Dsn = ""
	err := trd.Exec("SELECT * FROM test")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(writer.table)
}
