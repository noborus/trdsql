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
	dbdriver := "sqlite3"
	dbdsn := ""
	cfgfile := configOpen()
	cfg, err := loadConfig(cfgfile)
	if err == nil {
		fmt.Printf("err:%s", cfg.Dbdriver)
		dbdriver = cfg.Dbdriver
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `
Usage: %s [OPTIONS] [SQL]

Options:
`, os.Args[0])
		flag.PrintDefaults()
	}

	flag.StringVar(&odbdriver, "dbdriver", "", "database driver. default sqlite3")
	flag.StringVar(&odbdsn, "dbdsn", "", "database connection option.")
	flag.StringVar(&inSep, "input-delimiter", ",", "Field delimiter for input.")
	flag.StringVar(&inSep, "d", ",", "Field delimiter for input.")
	flag.StringVar(&outSep, "output-delimiter", ",", "Field delimiter for output.")
	flag.StringVar(&outSep, "D", ",", "Field delimiter for output.")
	flag.Parse()
	if odbdriver != "" {
		dbdriver = odbdriver
	}
	if odbdsn != "" {
		dbdsn = odbdsn
	}
	if len(flag.Args()) == 0 {
		flag.Usage()
		return (2)
	}
	sqlstr := flag.Args()[0]
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

	db := dbConnect(dbdriver, dbdsn)
	defer db.dbDisconnect()

	tablenames := sqlparse(sqlstr)
	for _, tablename := range tablenames {
		reader, err := csvOpen(tablename)
		if err != nil {
			continue
		}
		rtable := escapetable(db, tablename)
		sqlstr = rewrite(sqlstr, tablename, rtable)
		reader.Comma = readerComma
		reader.FieldsPerRecord = -1
		header := csvRead(reader)
		db.dbCreate(rtable, header)
		db.dbImport(reader, rtable, header)
	}
	db.dbSelect(writer, sqlstr)
	return 0
}
