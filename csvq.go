package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
)

// Run is main routine.
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
	cfg, _ := loadConfig(cfgfile)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
Usage: %s [OPTIONS] [SQL]

Options:
`, os.Args[0])
		flags.PrintDefaults()
	}

	flags.StringVar(&cfg.Db, "db", cfg.Db, "db of the configuration file")
	flags.StringVar(&odbdriver, "dbdriver", "", "database driver. default sqlite3")
	flags.StringVar(&odbdsn, "dbdsn", "", "database connection option.")
	flags.StringVar(&inSep, "input-delimiter", ",", "Field delimiter for input.")
	flags.StringVar(&inSep, "d", ",", "Field delimiter for input.")
	flags.StringVar(&outSep, "output-delimiter", ",", "Field delimiter for output.")
	flags.StringVar(&outSep, "D", ",", "Field delimiter for output.")
	flags.Parse(args[1:])
	if len(flags.Args()) == 0 {
		flags.Usage()
		return (2)
	}
	sqlstr := flags.Args()[0]
	if cfg.Db != "" {
		for _, c := range cfg.Database {
			if cfg.Db == c.Name {
				dbdriver = c.Dbdriver
				dbdsn = c.Dsn
			}
		}
	}
	if odbdriver != "" {
		dbdriver = odbdriver
	}
	if odbdsn != "" {
		dbdsn = odbdsn
	}
	if dbdriver == "sqlite3" && dbdsn == "" {
		dbdsn = ":memory:"
	}

	readerComma, err := getSeparator(inSep)
	if err != nil {
		log.Println(err)
	}
	log.Printf("driver: %s, dsn: %s", dbdriver, dbdsn)
	db, err := Connect(dbdriver, dbdsn)
	if err != nil {
		log.Println("ERROR: ", err)
		return 1
	}
	defer db.Disconnect()

	tablenames := sqlparse(sqlstr)
	if len(tablenames) == 0 {
		log.Println("ERROR: table not found")
		return 1
	}
	var reader *csv.Reader
	var header []string
	for _, tablename := range tablenames {
		reader, err = csvOpen(tablename)
		if err != nil {
			// no file
			continue
		}
		rtable := db.escapetable(tablename)
		sqlstr = rewrite(sqlstr, tablename, rtable)
		reader.Comma = readerComma
		header, err = headerRead(reader)
		if err != nil {
			log.Println(err)
			return 1
		}
		db.Create(rtable, header)
		db.Import(reader, rtable, header)
	}
	writer := csv.NewWriter(csvq.outStream)
	writer.Comma, err = getSeparator(outSep)
	if err != nil {
		log.Println(err)
	}
	err = db.Select(writer, sqlstr)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}
