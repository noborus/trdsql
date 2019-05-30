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

// TRDSQL structure is a structure that defines the whole operation.
type TRDSQL struct {
	Driver string
	Dsn    string

	InFormat    InputFormat
	InPreRead   int
	InSkip      int
	InDelimiter string
	InHeader    bool

	OutStream    io.Writer
	ErrStream    io.Writer
	OutDelimiter string
	OutHeader    bool
}

// InputFormat represents the input format
type InputFormat int

// Represents Input Format
const (
	GUESS = iota
	CSV
	LTSV
	JSON
	TBLN
)

// Run is main routine.
func (trdsql *TRDSQL) Run(args []string) int {
	var (
		usage   bool
		version bool
		dbList  bool
		config  string
		cDB     string
		cDriver string
		cDSN    string
		guess   bool
		query   string
		odebug  bool
	)

	// input Format
	var (
		iCSV  bool
		iLTSV bool
		iJSON bool
		iTBLN bool
	)

	// Output Format
	var (
		oCSV  bool
		oLTSV bool
		oAT   bool
		oMD   bool
		oVF   bool
		oRAW  bool
		oJSON bool
		oTBLN bool
	)

	flags := flag.NewFlagSet("trdsql", flag.ExitOnError)
	trdsql.Driver = "sqlite3"
	trdsql.Dsn = ""
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s [OPTIONS] [SQL(SELECT...)]
`, os.Args[0])
		fmt.Fprintf(os.Stderr, `'See %s -help'
`, os.Args[0])
	}

	flags.StringVar(&config, "config", config, "Configuration file location.")
	flags.StringVar(&cDB, "db", "", "Specify db name of the setting.")
	flags.BoolVar(&dbList, "dblist", false, "display db information.")
	flags.StringVar(&cDriver, "driver", "", "database driver.  [ "+strings.Join(sql.Drivers(), " | ")+" ]")
	flags.StringVar(&cDSN, "dsn", "", "database connection option.")
	flags.BoolVar(&guess, "ig", true, "Guess format from extension.")
	flags.BoolVar(&iCSV, "icsv", false, "CSV format for input.")
	flags.BoolVar(&iLTSV, "iltsv", false, "LTSV format for input.")
	flags.BoolVar(&iJSON, "ijson", false, "JSON format for input.")
	flags.BoolVar(&iTBLN, "itbln", false, "TBLN format for input.")
	flags.StringVar(&trdsql.InDelimiter, "id", ",", "Field delimiter for input.")
	flags.StringVar(&trdsql.OutDelimiter, "od", ",", "Field delimiter for output.")
	flags.BoolVar(&trdsql.InHeader, "ih", false, "The first line is interpreted as column names(CSV only).")
	flags.IntVar(&trdsql.InSkip, "is", 0, "Skip header row.")
	flags.IntVar(&trdsql.InPreRead, "ir", 1, "Number of row preread for column determination.")
	flags.BoolVar(&trdsql.OutHeader, "oh", false, "Output column name as header.")
	flags.StringVar(&query, "q", "", "Read query from the provided filename.")
	flags.BoolVar(&usage, "help", false, "display usage information.")
	flags.BoolVar(&version, "version", false, "display version information.")
	flags.BoolVar(&odebug, "debug", false, "debug print.")

	flags.BoolVar(&oCSV, "ocsv", true, "CSV format for output.")
	flags.BoolVar(&oLTSV, "oltsv", false, "LTSV format for output.")
	flags.BoolVar(&oAT, "oat", false, "ASCII Table format for output.")
	flags.BoolVar(&oMD, "omd", false, "Mark Down format for output.")
	flags.BoolVar(&oVF, "ovf", false, "Vertical format for output.")
	flags.BoolVar(&oRAW, "oraw", false, "Raw format for output.")
	flags.BoolVar(&oJSON, "ojson", false, "JSON format for output.")
	flags.BoolVar(&oTBLN, "otbln", false, "TBLN format for output.")

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

	switch {
	case iCSV:
		trdsql.InFormat = CSV
	case iLTSV:
		trdsql.InFormat = LTSV
	case iJSON:
		trdsql.InFormat = JSON
	case iTBLN:
		trdsql.InFormat = TBLN
	default:
		trdsql.InFormat = GUESS
	}

	cfgFile := configOpen(config)
	cfg, err := loadConfig(cfgFile)
	if err != nil {
		if config != "" {
			log.Printf("ERROR: [%s]%s", config, err)
			return (1)
		}
	}
	if dbList {
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

	trdsql.setDB(cfg, cDB, cDriver, cDSN)

	var w Exporter
	switch {
	case oLTSV:
		w = trdsql.ltsvOutNew()
	case oJSON:
		w = trdsql.jsonOutNew()
	case oRAW:
		w = trdsql.rawOutNew()
	case oMD:
		w = trdsql.twOutNew(true)
	case oAT:
		w = trdsql.twOutNew(false)
	case oVF:
		w = trdsql.vfOutNew()
	case oTBLN:
		w = trdsql.tblnOutNew()
	case oCSV:
		w = trdsql.csvOutNew()
	default:
		w = trdsql.csvOutNew()
	}

	return trdsql.Exec(sqlstr, w)
}

func (trdsql *TRDSQL) Exec(sqlstr string, w Exporter) int {
	db, err := Connect(trdsql.Driver, trdsql.Dsn)
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

	err = trdsql.Export(db, sqlstr, w)
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

func getSQL(args []string, query string) (string, error) {
	sqlstr := ""
	if query != "" {
		bq, err := ioutil.ReadFile(query)
		if err != nil {
			return "", err
		}
		sqlstr = string(bq)
	} else {
		sqlstr = strings.Join(args, " ")
	}
	if strings.HasSuffix(sqlstr, ";") {
		sqlstr = sqlstr[:len(sqlstr)-1]
	}
	return sqlstr, nil
}

func (trdsql *TRDSQL) setDB(cfg *config, cDB string, cDriver string, cDSN string) {
	if cDB == "" {
		cDB = cfg.Db
	}
	if cDB != "" {
		if cfg.Database[cDB].Driver == "" {
			debug.Printf("ERROR: db[%s] does not found", cDB)
		} else {
			trdsql.Driver = cfg.Database[cDB].Driver
			trdsql.Dsn = cfg.Database[cDB].Dsn
		}
	}
	if debug {
		for od, odb := range cfg.Database {
			if cDB == od {
				debug.Printf(">[driver: %s:%s:%s]", od, odb.Driver, odb.Dsn)
			} else {
				debug.Printf(" [driver: %s:%s:%s]", od, odb.Driver, odb.Dsn)
			}
		}
	}
	if cDriver != "" {
		trdsql.Driver = cDriver
		trdsql.Dsn = cDSN
	}
	if cDSN != "" {
		trdsql.Dsn = cDSN
	}
	debug.Printf("driver: %s, dsn: %s", trdsql.Driver, trdsql.Dsn)
}

var debug = debugT(false)

type debugT bool

func (d debugT) Printf(format string, args ...interface{}) {
	if d {
		log.Printf(format, args...)
	}
}
