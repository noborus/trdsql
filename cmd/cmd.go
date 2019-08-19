package cmd

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/noborus/trdsql"
)

// input format flag
type inputFlag struct {
	CSV  bool
	LTSV bool
	JSON bool
	TBLN bool
}

// Output format flag
type outputFlag struct {
	CSV  bool
	LTSV bool
	AT   bool
	MD   bool
	VF   bool
	RAW  bool
	JSON bool
	TBLN bool
}

func inputFormat(i inputFlag) trdsql.Format {
	switch {
	case i.CSV:
		return trdsql.CSV
	case i.LTSV:
		return trdsql.LTSV
	case i.JSON:
		return trdsql.JSON
	case i.TBLN:
		return trdsql.TBLN
	default:
		return trdsql.GUESS
	}
}

func outputFormat(o outputFlag) trdsql.Format {
	switch {
	case o.LTSV:
		return trdsql.LTSV
	case o.JSON:
		return trdsql.JSON
	case o.RAW:
		return trdsql.RAW
	case o.MD:
		return trdsql.MD
	case o.AT:
		return trdsql.AT
	case o.VF:
		return trdsql.VF
	case o.TBLN:
		return trdsql.TBLN
	case o.CSV:
		return trdsql.CSV
	default:
		return trdsql.CSV
	}
}

// Debug flag for a detailed output
var Debug bool

// Run is main routine.
func Run(args []string) int {
	var (
		usage     bool
		version   bool
		dbList    bool
		config    string
		cDB       string
		cDriver   string
		cDSN      string
		guess     bool
		queryFile string

		inFlag      inputFlag
		inDelimiter string
		inHeader    bool
		inSkip      int
		inPreRead   int

		outFlag      outputFlag
		outDelimiter string
		outHeader    bool
	)

	flags := flag.NewFlagSet("trdsql", flag.ExitOnError)

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
	flags.StringVar(&queryFile, "q", "", "Read query from the provided filename.")
	flags.BoolVar(&usage, "help", false, "display usage information.")
	flags.BoolVar(&version, "version", false, "display version information.")
	flags.BoolVar(&Debug, "debug", false, "debug print.")

	flags.StringVar(&inDelimiter, "id", ",", "Field delimiter for input.")
	flags.BoolVar(&inHeader, "ih", false, "The first line is interpreted as column names(CSV only).")
	flags.IntVar(&inSkip, "is", 0, "Skip header row.")
	flags.IntVar(&inPreRead, "ir", 1, "Number of row preread for column determination.")

	flags.BoolVar(&inFlag.CSV, "icsv", false, "CSV format for input.")
	flags.BoolVar(&inFlag.LTSV, "iltsv", false, "LTSV format for input.")
	flags.BoolVar(&inFlag.JSON, "ijson", false, "JSON format for input.")
	flags.BoolVar(&inFlag.TBLN, "itbln", false, "TBLN format for input.")

	flags.StringVar(&outDelimiter, "od", ",", "Field delimiter for output.")
	flags.BoolVar(&outHeader, "oh", false, "Output column name as header.")

	flags.BoolVar(&outFlag.CSV, "ocsv", true, "CSV format for output.")
	flags.BoolVar(&outFlag.LTSV, "oltsv", false, "LTSV format for output.")
	flags.BoolVar(&outFlag.AT, "oat", false, "ASCII Table format for output.")
	flags.BoolVar(&outFlag.MD, "omd", false, "Mark Down format for output.")
	flags.BoolVar(&outFlag.VF, "ovf", false, "Vertical format for output.")
	flags.BoolVar(&outFlag.RAW, "oraw", false, "Raw format for output.")
	flags.BoolVar(&outFlag.JSON, "ojson", false, "JSON format for output.")
	flags.BoolVar(&outFlag.TBLN, "otbln", false, "TBLN format for output.")

	err := flags.Parse(args[1:])
	if err != nil {
		log.Println("ERROR:", err)
		return 1
	}

	if version {
		fmt.Println(trdsql.Version)
		return 0
	}

	if Debug {
		trdsql.EnableDebug()
	}

	cfgFile := configOpen(config)
	cfg, err := loadConfig(cfgFile)
	if err != nil {
		if config != "" {
			log.Printf("ERROR: [%s]%s", config, err)
			return 1
		}
	}
	if dbList {
		for od, odb := range cfg.Database {
			fmt.Printf("%s:%s\n", od, odb.Driver)
		}
		return 0
	}

	query, err := getQuery(flags.Args(), queryFile)
	if err != nil {
		log.Printf("ERROR: %s", err)
		return 1
	}

	if usage || (len(query) == 0) {
		fmt.Fprintf(os.Stderr, `
Usage: %s [OPTIONS] [SQL(SELECT...)]

Options:
`, os.Args[0])
		flags.PrintDefaults()
		return 2
	}

	importer := trdsql.NewImporter(
		trdsql.InFormat(inputFormat(inFlag)),
		trdsql.InDelimiter(inDelimiter),
		trdsql.InHeader(inHeader),
		trdsql.InSkip(inSkip),
		trdsql.InPreRead(inPreRead),
	)

	w := trdsql.NewWriter(
		trdsql.OutFormat(outputFormat(outFlag)),
		trdsql.OutDelimiter(outDelimiter),
		trdsql.OutHeader(outHeader),
	)
	exporter := trdsql.NewExporter(w)

	trd := trdsql.NewTRDSQL(importer, exporter)

	driver, dsn := getDB(cfg, cDB, cDriver, cDSN)
	if driver != "" {
		trd.Driver = driver
	}
	if dsn != "" {
		trd.Dsn = dsn
	}

	err = trd.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
	return 0
}

func getQuery(args []string, fileName string) (string, error) {
	query := ""
	if fileName != "" {
		sqlByte, err := ioutil.ReadFile(fileName)
		if err != nil {
			return "", err
		}
		query = string(sqlByte)
	} else {
		query = strings.Join(args, " ")
	}
	if strings.HasSuffix(query, ";") {
		query = query[:len(query)-1]
	}
	return query, nil
}

func getDB(cfg *config, cDB string, cDriver string, cDSN string) (string, string) {
	if cDB == "" {
		cDB = cfg.Db
	}
	if Debug {
		for od, odb := range cfg.Database {
			if cDB == od {
				log.Printf(">[driver: %s:%s:%s]", od, odb.Driver, odb.Dsn)
			} else {
				log.Printf(" [driver: %s:%s:%s]", od, odb.Driver, odb.Dsn)
			}
		}
	}
	if cDB != "" {
		if cfg.Database[cDB].Driver == "" {
			log.Printf("ERROR: db[%s] does not found", cDB)
		} else {
			return cfg.Database[cDB].Driver, cfg.Database[cDB].Dsn
		}
	}
	if cDriver != "" {
		return cDriver, cDSN
	}
	if cDSN != "" {
		return "", cDSN
	}
	return "", ""
}
