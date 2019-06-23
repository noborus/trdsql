package trdsql_test

import (
	"bytes"
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

func ExampleBufferImporter() {
	jsonString := `
	[
		{
		  "name": "Sarah Carpenter",
		  "gender": "female",
		  "company": "ACCUSAGE",
		  "tags": [
			"veniam",
			"exercitation",
			"nulla",
			"anim",
			"ea",
			"ullamco",
			"ut"
		  ],
		  "greeting": "Hello, Sarah Carpenter! You have 1 unread messages."
		},
		{
		  "name": "Perez Atkinson",
		  "gender": "male",
		  "company": "JOVIOLD",
		  "tags": [
			"minim",
			"adipisicing",
			"ad",
			"occaecat",
			"incididunt",
			"eu",
			"esse"
		  ],
		  "greeting": "Hello, Perez Atkinson! You have 10 unread messages."
		},
		{
		  "name": "Valeria Potts",
		  "gender": "female",
		  "company": "EXOZENT",
		  "tags": [
			"esse",
			"pariatur",
			"nisi",
			"commodo",
			"adipisicing",
			"ut",
			"consectetur"
		  ],
		  "greeting": "Hello, Valeria Potts! You have 8 unread messages."
		}
	  ]
`
	r := bytes.NewBufferString(jsonString)
	importer, err := trdsql.NewBufferImporter("test", r, trdsql.InFormat(trdsql.JSON))
	if err != nil {
		log.Fatal(err)
	}
	writer := trdsql.NewWriter(
		trdsql.OutFormat(trdsql.CSV),
		trdsql.OutDelimiter("\t"),
	)
	trd := trdsql.NewTRDSQL(importer, trdsql.NewExporter(writer))
	err = trd.Exec("SELECT name,gender,company FROM test")
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	//Sarah Carpenter	female	ACCUSAGE
	//Perez Atkinson	male	JOVIOLD
	//Valeria Potts	female	EXOZENT
}
