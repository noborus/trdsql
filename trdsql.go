package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

const VERSION = `0.0.1`

var debug = debugT(false)

type debugT bool

func (d debugT) Printf(format string, args ...interface{}) {
	if d {
		log.Printf(format, args...)
	}
}

// Run is main routine.
func (trdsql TRDSQL) Run(args []string) int {
	var (
		version   bool
		odbdriver string
		odbdsn    string
		inSep     string
		outSep    string
		ihead     bool
		ohead     bool
		iskip     int
		odebug    bool
	)
	flags := flag.NewFlagSet("trdsql", flag.ContinueOnError)
	dbdriver := "sqlite3"
	dbdsn := ""
	cfgfile := configOpen()
	cfg, _ := loadConfig(cfgfile)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
Usage: %s [OPTIONS] [SQL(SELECT...)]

Options:
`, os.Args[0])
		flags.PrintDefaults()
	}

	flags.StringVar(&cfg.Db, "db", cfg.Db, "Specify db name of the setting.")
	flags.StringVar(&odbdriver, "dbdriver", "", "database driver. default sqlite3")
	flags.StringVar(&odbdsn, "dbdsn", "", "database connection option.")
	flags.StringVar(&inSep, "id", ",", "Field delimiter for input.")
	flags.StringVar(&outSep, "od", ",", "Field delimiter for output.")
	flags.BoolVar(&ihead, "ih", false, "The first line is interpreted as column names.")
	flags.BoolVar(&ohead, "oh", false, "Output column name as header.")
	flags.IntVar(&iskip, "is", 0, "Skip header.")
	flags.BoolVar(&version, "version", false, "display version information.")
	flags.BoolVar(&odebug, "debug", false, "debug print.")
	flags.Parse(args[1:])
	if version {
		fmt.Println(VERSION)
		return (0)
	}
	if len(flags.Args()) == 0 {
		flags.Usage()
		return (2)
	}
	if odebug {
		debug = true
	}
	sqlstr := flags.Args()[0]
	if strings.HasSuffix(sqlstr, ";") {
		sqlstr = sqlstr[:len(sqlstr)-1]
	}

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
	debug.Printf("driver: %s, dsn: %s", dbdriver, dbdsn)
	db, err := Connect(dbdriver, dbdsn)
	if err != nil {
		log.Println("ERROR: ", err)
		return 1
	}
	defer db.Disconnect()

	tablenames := sqlparse(sqlstr)
	if len(tablenames) == 0 {
		// withou FROM clause. ex. SELECT 1+1;
		debug.Printf("table not found\n")
	}
	var reader *csv.Reader
	var header []string
	for _, tablename := range tablenames {
		reader, err = csvOpen(tablename, iskip)
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
		db.Create(rtable, header, ihead)
		db, err = db.ImportPrepare(rtable, header, ihead)
		if err != nil {
			log.Println(err)
			return 1
		}
		db.Import(reader, header, ihead)
	}
	writer := csv.NewWriter(trdsql.outStream)
	writer.Comma, err = getSeparator(outSep)
	if err != nil {
		log.Println(err)
	}
	err = db.Select(writer, sqlstr, ohead)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}
