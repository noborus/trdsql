package main

import (
	"bytes"
	"log"

	"github.com/noborus/trdsql"
)

func main() {
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
	trdsql.EnableDebug()
	r := bytes.NewBufferString(jsonString)
	importer, err := trdsql.NewBufferImporter("test", r, trdsql.InFormat(trdsql.JSON))
	if err != nil {
		log.Fatal(err)
	}
	writer := trdsql.NewWriter(trdsql.OutFormat(trdsql.VF))
	trd := trdsql.NewTRDSQL(importer, trdsql.NewExporter(writer))
	trd.Driver = "postgres"
	err = trd.Exec("SELECT name,gender,company,tags,greeting FROM test")
	if err != nil {
		log.Fatal(err)
	}
}
