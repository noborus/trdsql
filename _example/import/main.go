package main

import (
	"log"

	"github.com/noborus/trdsql"
)

type importer struct {
}

func (f *importer) Import(db *trdsql.DB, query string) (string, error) {
	err := db.CreateTable("test", []string{"a"}, []string{"text"})
	if err != nil {
		return query, err
	}
	_, err = db.Tx.Exec("INSERT INTO test (a) VALUES ('aaaa')")
	if err != nil {
		return query, err
	}
	return query, err
}

func main() {
	trdsql.EnableDebug()
	d := importer{}
	writeOpts := trdsql.NewWriteOpts()

	trd := trdsql.NewTRDSQL(&d, trdsql.NewExporter(writeOpts, trdsql.NewWriter(writeOpts)))
	err := trd.Exec("SELECT * FROM test")
	if err != nil {
		log.Fatal(err)
	}

}
