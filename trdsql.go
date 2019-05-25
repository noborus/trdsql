package trdsql

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
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

// (Default)input Formast
var (
	Icsv  bool
	Iltsv bool
	Ijson bool
	Itbln bool
)

// Output Formast
var (
	Ocsv  bool
	Oltsv bool
	Oat   bool
	Omd   bool
	Ovf   bool
	Oraw  bool
	Ojson bool
	Otbln bool
)

// Input format
const (
	CSV = iota
	LTSV
	JSON
	TBLN
)

// TRDSQL is output stream define
type TRDSQL struct {
	OutStream    io.Writer
	ErrStream    io.Writer
	driver       string
	dsn          string
	inDelimiter  string
	inSkip       int
	inGuess      bool
	inType       int
	inHeader     bool
	inPreRead    int
	outDelimiter string
	outHeader    bool
}

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
		fmt.Fprintf(os.Stderr, `Usage: %s [OPTIONS] [SQL(SELECT...)]
`, os.Args[0])
		fmt.Fprintf(os.Stderr, `'See %s -help'
`, os.Args[0])
	}

	flags.StringVar(&config, "config", config, "Configuration file location.")
	flags.StringVar(&cdb, "db", "", "Specify db name of the setting.")
	flags.BoolVar(&dblist, "dblist", false, "display db information.")
	flags.StringVar(&cdriver, "driver", "", "database driver.  [ "+strings.Join(sql.Drivers(), " | ")+" ]")
	flags.StringVar(&cdsn, "dsn", "", "database connection option.")
	flags.BoolVar(&trdsql.inGuess, "ig", false, "Guess format from extension.")
	flags.BoolVar(&Icsv, "icsv", false, "CSV format for input.")
	flags.BoolVar(&Iltsv, "iltsv", false, "LTSV format for input.")
	flags.BoolVar(&Ijson, "ijson", false, "JSON format for input.")
	flags.BoolVar(&Itbln, "itbln", false, "TBLN format for input.")
	flags.StringVar(&trdsql.inDelimiter, "id", ",", "Field delimiter for input.")
	flags.StringVar(&trdsql.outDelimiter, "od", ",", "Field delimiter for output.")
	flags.BoolVar(&trdsql.inHeader, "ih", false, "The first line is interpreted as column names(CSV only).")
	flags.IntVar(&trdsql.inSkip, "is", 0, "Skip header row.")
	flags.IntVar(&trdsql.inPreRead, "ir", 1, "Number of row preread for column determination.")
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
	flags.BoolVar(&Otbln, "otbln", false, "TBLN format for output.")

	err := flags.Parse(args[1:])
	if err != nil {
		log.Println("ERROR:", err)
		return (1)
	}

	if version {
		fmt.Println(VERSION)
		return (0)
	}

	if odebug {
		debug = true
	}

	trdsql.setInFormat()

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

	sqlstr, err := getSQL(flags.Args(), query)
	if err != nil {
		log.Printf("ERROR: %s", err)
		return (1)
	}

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
		log.Printf("ERROR(CONNECT):%s", err)
		return 1
	}
	defer func() {
		err = db.Disconnect()
		if err != nil {
			log.Printf("ERROR(DISCONNECT):%s", err)
		}
	}()

	db.tx, err = db.Begin()
	if err != nil {
		log.Printf("ERROR(BEGIN):%s", err)
		return 1
	}

	sqlstr, err = trdsql.Import(db, sqlstr)
	if err != nil {
		log.Printf("ERROR(IMPORT):%s", err)
		return 1
	}

	err = trdsql.Export(db, sqlstr, output)
	if err != nil {
		log.Printf("ERROR(EXPORT):%s", err)
		return 1
	}

	err = db.tx.Commit()
	if err != nil {
		log.Printf("ERROR(COMMIT):%s", err)
		return 1
	}

	return 0
}

func (trdsql *TRDSQL) setInFormat() {
	switch {
	case Icsv:
		trdsql.inType = CSV
	case Iltsv:
		trdsql.inType = LTSV
	case Ijson:
		trdsql.inType = JSON
	case Itbln:
		trdsql.inType = TBLN
	default:
		trdsql.inType = CSV
	}
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
		output = trdsql.twOutNew(true)
	case Oat:
		output = trdsql.twOutNew(false)
	case Ovf:
		output = trdsql.vfOutNew()
	case Otbln:
		output = trdsql.tblnOutNew()
	case Ocsv:
		output = trdsql.csvOutNew()
	default:
		output = trdsql.csvOutNew()
	}
	return output
}

func getSQL(rargs []string, query string) (string, error) {
	sqlstr := ""
	if query != "" {
		bq, err := ioutil.ReadFile(query)
		if err != nil {
			return "", err
		}
		sqlstr = string(bq)
	} else {
		sqlstr = strings.Join(rargs, " ")
	}
	if strings.HasSuffix(sqlstr, ";") {
		sqlstr = sqlstr[:len(sqlstr)-1]
	}
	return sqlstr, nil
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
		trdsql.dsn = cdsn
	}
	if cdsn != "" {
		trdsql.dsn = cdsn
	}
	debug.Printf("driver: %s, dsn: %s", trdsql.driver, trdsql.dsn)
}
