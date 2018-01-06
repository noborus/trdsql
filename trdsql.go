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

// Output Formast
var (
	Ocsv  bool
	Oltsv bool
	Oat   bool
	Omd   bool
	Ovf   bool
	Oraw  bool
	Ojson bool
)

// Run is main routine.
func (trdsql *TRDSQL) Run(args []string) int {
	var (
		usage   bool
		version bool
		dblist  bool
		config  string
		cdb     string
		cdriver string
		cdsn    string
		query   string
		odebug  bool
	)

	var output Output

	flags := flag.NewFlagSet("trdsql", flag.ExitOnError)
	trdsql.driver = "sqlite3"
	trdsql.dsn = ""
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, `[OPTIONS] [SQL(SELECT...)]`)
		fmt.Fprintf(os.Stderr, `'See %s -help'
`, os.Args[0])
	}

	flags.StringVar(&config, "config", config, "Configuration file location.")
	flags.StringVar(&cdb, "db", "", "Specify db name of the setting.")
	flags.BoolVar(&dblist, "dblist", false, "display db information.")
	flags.StringVar(&cdriver, "driver", "", "database driver.  [ "+strings.Join(sql.Drivers(), " | ")+" ]")
	flags.StringVar(&cdsn, "dsn", "", "database connection option.")
	flags.BoolVar(&trdsql.iguess, "ig", false, "Guess format from extension.")
	flags.BoolVar(&trdsql.icsv, "icsv", false, "CSV format for input.")
	flags.BoolVar(&trdsql.iltsv, "iltsv", false, "LTSV format for input.")
	flags.BoolVar(&trdsql.ijson, "ijson", false, "JSON format for input.")
	flags.StringVar(&trdsql.inSep, "id", ",", "Field delimiter for input.")
	flags.StringVar(&trdsql.outSep, "od", ",", "Field delimiter for output.")
	flags.BoolVar(&trdsql.ihead, "ih", false, "The first line is interpreted as column names(CSV only).")
	flags.IntVar(&trdsql.iskip, "is", 0, "Skip header row.")
	flags.BoolVar(&trdsql.outHeader, "oh", false, "Output column name as header.")
	flags.StringVar(&query, "q", "", "Read query from the provided filename.")
	flags.BoolVar(&usage, "help", false, "display usage information.")
	flags.BoolVar(&version, "version", false, "display version information.")
	flags.BoolVar(&odebug, "debug", false, "debug print.")

	flags.BoolVar(&Ocsv, "ocsv", true, "CSV format for output.")
	flags.BoolVar(&Oltsv, "oltsv", false, "LTSV format for output.")
	flags.BoolVar(&Oat, "oat", false, "ASCII Table format for output.")
	flags.BoolVar(&Omd, "omd", false, "Mark Down format for output.")
	flags.BoolVar(&Ovf, "ovf", false, "Vertical format for output.")
	flags.BoolVar(&Oraw, "oraw", false, "Raw format for output.")
	flags.BoolVar(&Ojson, "ojson", false, "JSON format for output.")

	flags.Parse(args[1:])

	if version {
		fmt.Println(VERSION)
		return (0)
	}

	if odebug {
		debug = true
	}

	cfgfile := configOpen(config)
	cfg, err := loadConfig(cfgfile)
	if err != nil {
		if config != "" {
			log.Printf("ERROR: [%s]%s", config, err)
			return (1)
		}
	}
	if dblist {
		for od, odb := range cfg.Database {
			fmt.Printf("%s:%s\n", od, odb.Driver)
		}
		return (0)
	}

	sqlstr := getSQL(flags.Args(), query)

	if usage || (len(sqlstr) == 0) {
		fmt.Fprintf(os.Stderr, `
Usage: %s [OPTIONS] [SQL(SELECT...)]

Options:
`, os.Args[0])
		flags.PrintDefaults()
		return (2)
	}

	trdsql.setDB(cfg, cdb, cdriver, cdsn)
	output = trdsql.setOutFormat()
	return trdsql.main(sqlstr, output)
}

func (trdsql *TRDSQL) main(sqlstr string, output Output) int {
	db, err := Connect(trdsql.driver, trdsql.dsn)
	if err != nil {
		log.Println("ERROR:", err)
		return 1
	}
	defer db.Disconnect()
	db.Tx, err = db.Begin()
	if err != nil {
		log.Println("ERROR:", err)
		return 1
	}

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
	err = db.Tx.Commit()
	if err != nil {
		log.Println("ERROR:", err)
		return 1
	}

	return 0
}

func (trdsql *TRDSQL) setOutFormat() Output {
	var output Output
	switch {
	case Oltsv:
		output = trdsql.ltsvOutNew()
	case Ojson:
		output = trdsql.jsonOutNew()
	case Oraw:
		output = trdsql.rawOutNew()
	case Omd:
		trdsql.omd = true
		output = trdsql.twOutNew()
	case Oat:
		output = trdsql.twOutNew()
	case Ovf:
		output = trdsql.vfOutNew()
	case Ocsv:
		output = trdsql.csvOutNew()
	default:
		output = trdsql.csvOutNew()
	}
	return output
}

func getSQL(rargs []string, query string) string {
	sqlstr := ""
	if query != "" {
		bq, err := ioutil.ReadFile(query)
		if err != nil {
			log.Println("ERROR:", err)
			return ""
		}
		sqlstr = string(bq)
	} else {
		sqlstr = strings.Join(rargs, " ")
	}
	if strings.HasSuffix(sqlstr, ";") {
		sqlstr = sqlstr[:len(sqlstr)-1]
	}
	return sqlstr
}

func (trdsql *TRDSQL) setDB(cfg *config, cdb string, cdriver string, cdsn string) {
	if cdb == "" {
		cdb = cfg.Db
	}
	if cdb != "" {
		if cfg.Database[cdb].Driver == "" {
			debug.Printf("ERROR: db[%s] does not found", cdb)
		} else {
			trdsql.driver = cfg.Database[cdb].Driver
			trdsql.dsn = cfg.Database[cdb].Dsn
		}
	}
	if debug {
		for od, odb := range cfg.Database {
			if cdb == od {
				debug.Printf(">[driver: %s:%s:%s]", od, odb.Driver, odb.Dsn)
			} else {
				debug.Printf(" [driver: %s:%s:%s]", od, odb.Driver, odb.Dsn)
			}
		}
	}
	if cdriver != "" {
		trdsql.driver = cdriver
	}
	if cdsn != "" {
		trdsql.dsn = cdsn
	}
	debug.Printf("driver: %s, dsn: %s", trdsql.driver, trdsql.dsn)
}
