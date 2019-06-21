package trdsql_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/noborus/trdsql"
)

func Example() {
	in := []byte(`"Rob","Pike",rob
Ken,Thompson,ken
"Robert","Griesemer","gri"
`)
	tmpfile, err := ioutil.TempFile("/tmp", "xxx")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		defer os.Remove(tmpfile.Name())
	}()
	_, err = tmpfile.Write(in)
	if err != nil {
		log.Fatal(err)
	}
	trd := trdsql.NewTRDSQL(
		trdsql.NewImporter(),
		trdsql.NewExporter(trdsql.NewWriter()),
	)
	query := fmt.Sprintf("SELECT c1 FROM %s ORDER BY c1", tmpfile.Name())
	err = trd.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	// Ken
	// Rob
	// Robert
}

func Example_options() {
	in := []byte(`first_name,last_name,username
"Rob","Pike",rob
Ken,Thompson,ken
"Robert","Griesemer","gri"
`)
	tmpfile, err := ioutil.TempFile("/tmp", "xxx")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		defer os.Remove(tmpfile.Name())
	}()
	_, err = tmpfile.Write(in)
	if err != nil {
		log.Fatal(err)
	}

	// NewImporter
	importer := trdsql.NewImporter(
		trdsql.InFormat(trdsql.CSV),
		trdsql.InHeader(true),
	)

	// NewWriter & NewExporter
	writer := trdsql.NewWriter(
		trdsql.OutFormat(trdsql.JSON),
	)
	exporter := trdsql.NewExporter(writer)

	trd := trdsql.NewTRDSQL(importer, exporter)
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY username", tmpfile.Name())
	err = trd.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	//[
	//   {
	//     "first_name": "Robert",
	//     "last_name": "Griesemer",
	//     "username": "gri"
	//   },
	//   {
	//     "first_name": "Ken",
	//     "last_name": "Thompson",
	//     "username": "ken"
	//   },
	//   {
	//     "first_name": "Rob",
	//     "last_name": "Pike",
	//     "username": "rob"
	//   }
	//]
}

func ExampleSliceImporter() {
	data := []struct {
		id   int
		name string
	}{
		{id: 1, name: "Bod"},
		{id: 2, name: "Alice"},
		{id: 3, name: "Henry"},
	}
	tableName := "slice"
	importer := trdsql.NewSliceImporter(tableName, data)
	trd := trdsql.NewTRDSQL(importer, trdsql.NewExporter(trdsql.NewWriter()))

	err := trd.Exec("SELECT name,id FROM slice ORDER BY id DESC")
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	//Henry,3
	//Alice,2
	//Bod,1
}

func ExampleSliceWriter() {
	data := []struct {
		id   int
		name string
	}{
		{id: 1, name: "Bod"},
		{id: 2, name: "Alice"},
		{id: 3, name: "Henry"},
	}
	tableName := "slice"
	importer := trdsql.NewSliceImporter(tableName, data)
	writer := trdsql.NewSliceWriter()
	trd := trdsql.NewTRDSQL(importer, trdsql.NewExporter(writer))

	err := trd.Exec("SELECT name,id FROM slice ORDER BY id DESC")
	if err != nil {
		log.Fatal(err)
	}
	table := writer.Table
	fmt.Print(table)
	// Output:
	// [[Henry 3] [Alice 2] [Bod 1]]
}
