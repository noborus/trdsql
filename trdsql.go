package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

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
		query   string
		odebug  bool
	)
	var (
		oltsv bool
		oat   bool
		omd   bool
		oraw  bool
		ojson bool
	)
	var output Output

	flags := flag.NewFlagSet("trdsql", flag.ContinueOnError)
	trdsql.driver = "sqlite3"
	trdsql.dsn = ""
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
	flags.BoolVar(&trdsql.iguess, "ig", false, "Guess format from extension.")
	flags.BoolVar(&trdsql.iltsv, "iltsv", false, "LTSV format for input.")
	flags.StringVar(&trdsql.inSep, "id", ",", "Field delimiter for input.")
	flags.StringVar(&trdsql.outSep, "od", ",", "Field delimiter for output.")
	flags.BoolVar(&trdsql.ihead, "ih", false, "The first line is interpreted as column names.")
	flags.BoolVar(&oltsv, "oltsv", false, "LTSV format for output.")
	flags.BoolVar(&oat, "oat", false, "ASCII Table format for output.")
	flags.BoolVar(&omd, "omd", false, "Mark Down format for output.")
	flags.BoolVar(&oraw, "oraw", false, "Raw format for output.")
	flags.BoolVar(&ojson, "ojson", false, "JSON format for output.")
	flags.BoolVar(&trdsql.outHeader, "oh", false, "Output column name as header.")
	flags.IntVar(&trdsql.iskip, "is", 0, "Skip header row.")
	flags.StringVar(&query, "q", "", "Read query from the provided filename.")
	flags.BoolVar(&version, "version", false, "display version information.")
	flags.BoolVar(&odebug, "debug", false, "debug print.")
	flags.Parse(args[1:])
	if version {
		fmt.Println(VERSION)
		return (0)
	}
	sqlstr := ""
	if query != "" {
		bq, err := ioutil.ReadFile(query)
		if err != nil {
			log.Println("ERROR:", err)
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
			trdsql.driver = cfg.Database[cfg.Db].Driver
			trdsql.dsn = cfg.Database[cfg.Db].Dsn
		}
	}

	if odriver != "" {
		trdsql.driver = odriver
	}
	if odsn != "" {
		trdsql.dsn = odsn
	}
	debug.Printf("driver: %s, dsn: %s", trdsql.driver, trdsql.dsn)

	switch {
	case oltsv:
		output = trdsql.ltsvOutNew()
	case ojson:
		output = trdsql.jsonOutNew()
	case oraw:
		output = trdsql.rawOutNew()
	case omd:
		trdsql.omd = true
		output = trdsql.twOutNew()
	case oat:
		output = trdsql.twOutNew()
	default:
		output = trdsql.csvOutNew()
	}
	return trdsql.main(sqlstr, output)
}

func (trdsql TRDSQL) main(sqlstr string, output Output) int {
	db, err := Connect(trdsql.driver, trdsql.dsn)
	if err != nil {
		log.Println("ERROR:", err)
		return 1
	}
	defer db.Disconnect()
	sqlstr, err = trdsql.dbimport(db, sqlstr)
	if err != nil {
		log.Println("ERROR:", err)
		return 1
	}
	err = trdsql.dbexport(db, sqlstr, output)
	if err != nil {
		log.Println("ERROR:", err)
		return 1
	}
	return 0
}
