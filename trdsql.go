package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
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
		iskip   int
		query   string
		driver  string
		dsn     string
		odebug  bool
	)
	type Format int
	var (
		oltsv bool
		oat   bool
		omd   bool
		oraw  bool
		fjson bool
	)
	flags := flag.NewFlagSet("trdsql", flag.ContinueOnError)
	driver = "sqlite3"
	dsn = ""
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
	flags.BoolVar(&fjson, "ojson", false, "JSON format for output.")
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

	debug.Printf("driver: %s, dsn: %s", driver, dsn)
	db, err := Connect(driver, dsn)
	if err != nil {
		log.Println("ERROR: ", err)
		return 1
	}
	defer db.Disconnect()

	tablenames := sqlparse(sqlstr)
	if len(tablenames) == 0 {
		// without FROM clause. ex. SELECT 1+1;
		debug.Printf("table not found\n")
	}
	trdsql.iskip = iskip
	sqlstr, err = trdsql.tableReader(db, sqlstr, tablenames)
	if err != nil {
		return 1
	}

	rows, err := db.Select(sqlstr)
	if err != nil {
		log.Println(err)
		return 1
	}
	switch {
	case oltsv:
		err = trdsql.ltsvWrite(rows)
	case fjson:
		err = trdsql.jsonWrite(rows)
	case oraw:
		err = trdsql.rawWrite(rows)
	case omd:
		trdsql.omd = true
		err = trdsql.twWrite(rows)
	case oat:
		err = trdsql.twWrite(rows)
	default:
		err = trdsql.csvWrite(rows)
	}
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}

func (trdsql TRDSQL) tableReader(db *DDB, sqlstr string, tablenames []string) (string, error) {
	var err error
	for _, tablename := range tablenames {
		ltsv := false
		if trdsql.iltsv {
			ltsv = true
		} else if trdsql.iguess {
			ltsv = guessExtension(tablename)
		}
		if ltsv {
			sqlstr, err = trdsql.ltsvReader(db, sqlstr, tablename)
		} else {
			sqlstr, err = trdsql.csvReader(db, sqlstr, tablename)
		}
	}
	return sqlstr, err
}

func guessExtension(tablename string) bool {
	pos := strings.LastIndex(tablename, ".")
	if pos > 0 && strings.ToLower(tablename[pos:]) == ".ltsv" {
		debug.Printf("%s is LTSV file", tablename)
		return true
	}
	debug.Printf("%s is CSV file", tablename)
	return false
}

func getSeparator(sepString string) (rune, error) {
	if sepString == "" {
		return 0, nil
	}
	sepRunes, err := strconv.Unquote(`'` + sepString + `'`)
	if err != nil {
		return ',', fmt.Errorf("ERROR getSeparator: %s:%s", err, sepString)
	}
	sepRune := ([]rune(sepRunes))[0]
	return sepRune, err
}

func tFileOpen(filename string) (*os.File, error) {
	if filename == "-" {
		return os.Stdin, nil
	}
	if filename[0] == '`' {
		filename = strings.Replace(filename, "`", "", 2)
	}
	if filename[0] == '"' {
		filename = strings.Replace(filename, "\"", "", 2)
	}
	return os.Open(filename)
}

func valString(v interface{}) string {
	var str string
	b, ok := v.([]byte)
	if ok {
		str = string(b)
	} else {
		if v == nil {
			str = ""
		} else {
			str = fmt.Sprint(v)
		}
	}
	return str
}

func write(rows *sql.Rows, columns []string, rowWrite func([]interface{})) error {
	var err error
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("ERROR: %s", err)
		}
		rowWrite(values)
	}
	return nil
}
