package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

const VERSION = `0.1.0`

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
		version bool
		odriver string
		odsn    string
		inSep   string
		ihead   bool
		iskip   int
		query   string
		driver  string
		dsn     string
		odebug  bool
	)
	flags := flag.NewFlagSet("trdsql", flag.ContinueOnError)
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
	flags.StringVar(&odriver, "driver", "", "database driver.  [ "+strings.Join(sql.Drivers(), " | ")+" ]")
	flags.StringVar(&odsn, "dsn", "", "database connection option.")
	flags.StringVar(&inSep, "id", ",", "Field delimiter for input.")
	flags.StringVar(&trdsql.outSep, "od", ",", "Field delimiter for output.")
	flags.BoolVar(&ihead, "ih", false, "The first line is interpreted as column names.")
	flags.BoolVar(&trdsql.outHeader, "oh", false, "Output column name as header.")
	flags.IntVar(&iskip, "is", 0, "Skip header row.")
	flags.StringVar(&query, "q", "", "Read query from the provided filename.")
	flags.BoolVar(&version, "version", false, "display version information.")
	flags.BoolVar(&odebug, "debug", false, "debug print.")
	flags.Parse(args[1:])
	if version {
		fmt.Println(VERSION)
		return (0)
	}
	var sqlstr string
	if query != "" {
		bq, err := ioutil.ReadFile(query)
		if err != nil {
			log.Println("ERROR: ", err)
			return (1)
		}
		sqlstr = string(bq)
	} else {
		sqlstr = strings.Join(flags.Args(), " ")
	}
	if len(sqlstr) == 0 {
		flags.Usage()
		return (2)
	}
	if odebug {
		debug = true
	}
	if strings.HasSuffix(sqlstr, ";") {
		sqlstr = sqlstr[:len(sqlstr)-1]
	}

	if cfg.Db != "" {
		if cfg.Database[cfg.Db].Driver == "" {
			debug.Printf("ERROR: db[%s] does not found", cfg.Db)
		} else {
			driver = cfg.Database[cfg.Db].Driver
			dsn = cfg.Database[cfg.Db].Dsn
		}
	}
	if odriver != "" {
		driver = odriver
	}
	if odsn != "" {
		dsn = odsn
	}

	readerComma, err := getSeparator(inSep)
	if err != nil {
		log.Println(err)
	}
	debug.Printf("driver: %s, dsn: %s", driver, dsn)
	db, err := Connect(driver, dsn)
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
		err = db.ImportPrepare(rtable, header, ihead)
		if err != nil {
			log.Println(err)
			return 1
		}
		db.Import(reader, header, ihead)
	}

	return trdsql.write(db, sqlstr)
}

func (trdsql TRDSQL) write(db *DDB, sqlstr string) int {
	var err error
	writer := csv.NewWriter(trdsql.outStream)
	writer.Comma, err = getSeparator(trdsql.outSep)
	if err != nil {
		log.Println(err)
	}
	err = db.Output(writer, sqlstr, trdsql.outHeader)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}
