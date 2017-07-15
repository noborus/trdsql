package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
)

func (csvq CSVQ) Run(args []string) int {
	var (
		odbdriver string
		odbdsn    string
		inSep     string
		outSep    string
	)
	flags := flag.NewFlagSet("csvq", flag.ContinueOnError)
	dbdriver := "sqlite3"
	dbdsn := ""
	cfgfile := configOpen()
	cfg, err := loadConfig(cfgfile)
	if err == nil {
		fmt.Printf("err:%s", cfg.Dbdriver)
		dbdriver = cfg.Dbdriver
	}
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
Usage: %s [OPTIONS] [SQL]

Options:
`, os.Args[0])
		flags.PrintDefaults()
	}

	flags.StringVar(&odbdriver, "dbdriver", "", "database driver. default sqlite3")
	flags.StringVar(&odbdsn, "dbdsn", "", "database connection option.")
	flags.StringVar(&inSep, "input-delimiter", ",", "Field delimiter for input.")
	flags.StringVar(&inSep, "d", ",", "Field delimiter for input.")
	flags.StringVar(&outSep, "output-delimiter", ",", "Field delimiter for output.")
	flags.StringVar(&outSep, "D", ",", "Field delimiter for output.")
	flags.Parse(args[1:])
	if odbdriver != "" {
		dbdriver = odbdriver
	}
	if odbdsn != "" {
		dbdsn = odbdsn
	}
	if len(flags.Args()) == 0 {
		flags.Usage()
		return (2)
	}
	sqlstr := flags.Args()[0]
	if dbdsn == "" && cfg != nil {
		for _, c := range cfg.Target {
			if dbdriver == c.Name {
				log.Println(c.Name, c.Dsn)
				dbdsn = c.Dsn
			}
		}
		if dbdriver == "sqlite3" {
			dbdsn = ":memory:"
		}
	}

	writer := csv.NewWriter(csvq.outStream)
	writer.Comma = getSeparator(outSep)
	readerComma := getSeparator(inSep)

	db := Connect(dbdriver, dbdsn)
	defer db.Disconnect()

	tablenames := sqlparse(sqlstr)
	for _, tablename := range tablenames {
		reader, err := csvOpen(tablename)
		if err != nil {
			continue
		}
		rtable := escapetable(db, tablename)
		sqlstr = rewrite(sqlstr, tablename, rtable)
		reader.Comma = readerComma
		reader.FieldsPerRecord = -1 // no check count
		header := headerRead(reader)
		db.Create(rtable, header)
		db.Import(reader, rtable, header)
	}
	db.Select(writer, sqlstr)
	return 0
}
