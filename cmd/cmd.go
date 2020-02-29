package cmd

import (
	"compress/gzip"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/dsnet/compress/bzip2"
	"github.com/klauspost/compress/zstd"
	"github.com/noborus/trdsql"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
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
	CSV   bool
	LTSV  bool
	AT    bool
	MD    bool
	VF    bool
	RAW   bool
	JSON  bool
	TBLN  bool
	JSONL bool
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
	case o.JSONL:
		return trdsql.JSONL
	case o.CSV:
		return trdsql.CSV
	default:
		return trdsql.GUESS
	}
}

// Cli wraps stdout and error output specification.
type Cli struct {
	// OutStream is the output destination.
	OutStream io.Writer

	// ErrStream is the error output destination.
	ErrStream io.Writer
}

// Debug flag for a detailed output
var Debug bool

// Run executes the main routine.
// The return value is the exit code.
func (cli Cli) Run(args []string) int {
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
		analyze   string
		onlySQL   string

		inFlag      inputFlag
		inDelimiter string
		inHeader    bool
		inSkip      int
		inPreRead   int

		outFlag         outputFlag
		outFile         string
		outWithoutGuess bool
		outDelimiter    string
		outQuote        string
		outCompression  string
		outAllQuotes    bool
		outUseCRLF      bool
		outHeader       bool
	)

	flags := flag.NewFlagSet(trdsql.AppName, flag.ExitOnError)

	flags.StringVar(&config, "config", config, "Configuration file location.")
	flags.StringVar(&cDB, "db", "", "Specify db name of the setting.")
	flags.BoolVar(&dbList, "dblist", false, "display db information.")
	flags.StringVar(&cDriver, "driver", "", "database driver.  [ "+strings.Join(sql.Drivers(), " | ")+" ]")
	flags.StringVar(&cDSN, "dsn", "", "database connection option.")
	flags.BoolVar(&guess, "ig", true, "Guess format from extension.")
	flags.StringVar(&queryFile, "q", "", "Read query from the provided filename.")
	flags.StringVar(&analyze, "a", "", "Analyze file and suggest SQL.")
	flags.StringVar(&onlySQL, "A", "", "Analyze but only suggest SQL.")
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

	flags.StringVar(&outFile, "out", "", "Output file name.")
	flags.BoolVar(&outWithoutGuess, "out-without-guess", false, "Output without guessing from file name.")
	flags.StringVar(&outDelimiter, "od", ",", "Field delimiter for output.")
	flags.StringVar(&outQuote, "oq", "\"", "Quote character for output.")
	flags.BoolVar(&outAllQuotes, "oaq", false, "Enclose all fields in quotes for output.")
	flags.BoolVar(&outUseCRLF, "ocrlf", false, "Use CRLF for output.")
	flags.BoolVar(&outHeader, "oh", false, "Output column name as header.")
	flags.StringVar(&outCompression, "oz", "", "Compression[gzip,zstd,lz4,xz].")

	flags.BoolVar(&outFlag.CSV, "ocsv", false, "CSV format for output.")
	flags.BoolVar(&outFlag.LTSV, "oltsv", false, "LTSV format for output.")
	flags.BoolVar(&outFlag.AT, "oat", false, "ASCII Table format for output.")
	flags.BoolVar(&outFlag.MD, "omd", false, "Mark Down format for output.")
	flags.BoolVar(&outFlag.VF, "ovf", false, "Vertical format for output.")
	flags.BoolVar(&outFlag.RAW, "oraw", false, "Raw format for output.")
	flags.BoolVar(&outFlag.JSON, "ojson", false, "JSON format for output.")
	flags.BoolVar(&outFlag.TBLN, "otbln", false, "TBLN format for output.")
	flags.BoolVar(&outFlag.JSONL, "ojsonl", false, "JSON Lines format for output.")

	err := flags.Parse(args[1:])
	if err != nil {
		log.Printf("ERROR: %s", err)
		return 1
	}

	if version {
		fmt.Printf("%s version %s\n", trdsql.AppName, trdsql.Version)
		return 0
	}

	if Debug {
		trdsql.EnableDebug()
	}

	cfgFile := configOpen(config)
	cfg, err := loadConfig(cfgFile)
	if err != nil && config != "" {
		log.Printf("ERROR: [%s]%s", config, err)
		return 1
	}
	if dbList {
		printDBList(cfg)
		return 0
	}
	driver, dsn := getDB(cfg, cDB, cDriver, cDSN)

	if analyze != "" || onlySQL != "" {
		opts := trdsql.NewAnalyzeOpts()
		opts = colorOpts(opts)
		opts = quoteOpts(opts, driver)
		if onlySQL != "" {
			analyze = onlySQL
			opts.Detail = false
		}
		opts = optsCommand(opts, os.Args)
		if inHeader && inPreRead == 1 {
			inPreRead = 2
		}
		readOpts := trdsql.NewReadOpts(
			trdsql.InFormat(inputFormat(inFlag)),
			trdsql.InDelimiter(inDelimiter),
			trdsql.InHeader(inHeader),
			trdsql.InSkip(inSkip),
			trdsql.InPreRead(inPreRead))
		err := trdsql.Analyze(analyze, opts, readOpts)
		if err != nil {
			log.Printf("ERROR: %s", err)
			return 1
		}
		return 0
	}

	query, err := getQuery(flags.Args(), queryFile)
	if err != nil {
		log.Printf("ERROR: %s", err)
		return 1
	}

	if usage || (len(query) == 0) {
		fmt.Fprintf(cli.ErrStream, `
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

	writer := cli.OutStream
	if outFile != "" {
		writer, err = os.Create(outFile)
		if err != nil {
			log.Printf("%s", err)
			return 1
		}
	}
	outFormat := outputFormat(outFlag)
	if outFormat == trdsql.GUESS {
		if outWithoutGuess {
			outFormat = trdsql.CSV
		} else {
			outFormat = trdsql.GuessFormat(outFile)
		}
	}
	if outCompression == "" && !outWithoutGuess {
		outCompression = guessCompression(outFile)
	}
	writer, err = compressionWriter(writer, outCompression)
	if err != nil {
		log.Printf("%s", err)
		return 1
	}

	w := trdsql.NewWriter(
		trdsql.OutFormat(outFormat),
		trdsql.OutDelimiter(outDelimiter),
		trdsql.OutQuote(outQuote),
		trdsql.OutAllQuotes(outAllQuotes),
		trdsql.OutUseCRLF(outUseCRLF),
		trdsql.OutHeader(outHeader),
		trdsql.OutStream(writer),
		trdsql.ErrStream(cli.ErrStream),
	)
	exporter := trdsql.NewExporter(w)

	trd := trdsql.NewTRDSQL(importer, exporter)

	if driver != "" {
		trd.Driver = driver
	}
	if dsn != "" {
		trd.Dsn = dsn
	}

	err = trd.Exec(query)
	if err != nil {
		log.Printf("%s", err)
		return 1
	}

	if wc, ok := writer.(io.Closer); ok {
		err = wc.Close()
		if err != nil {
			log.Printf("%s", err)
			return 1
		}
	}
	return 0
}

func printDBList(cfg *config) {
	for od, odb := range cfg.Database {
		fmt.Printf("%s:%s\n", od, odb.Driver)
	}
}

func colorOpts(opts *trdsql.AnalyzeOpts) *trdsql.AnalyzeOpts {
	color := os.Getenv("NO_COLOR")
	if color != "" || runtime.GOOS == "windows" {
		opts.Color = false
	}
	return opts
}

func quoteOpts(opts *trdsql.AnalyzeOpts, driver string) *trdsql.AnalyzeOpts {
	if driver == "postgres" {
		opts.Quote = `\"`
	}
	return opts
}

func optsCommand(opts *trdsql.AnalyzeOpts, args []string) *trdsql.AnalyzeOpts {
	command := args[0]
	omitFlag := false
	for _, arg := range args[1:] {
		if omitFlag {
			omitFlag = false
			continue
		}
		if arg == "-a" || arg == "-A" {
			omitFlag = true
			continue
		}
		if len(arg) <= 1 || arg[0] != '-' {
			arg = quotedArg(arg)
		}
		command += " " + arg
	}
	opts.Command = command
	return opts
}

func trimQuery(query string) string {
	return strings.TrimRight(strings.TrimSpace(query), ";")
}

func getQuery(args []string, fileName string) (string, error) {
	if fileName == "" {
		return trimQuery(strings.Join(args, " ")), nil
	}

	sqlByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		return "", err
	}
	return trimQuery(string(sqlByte)), nil
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
	if cDriver != "" {
		return cDriver, cDSN
	}
	if cDSN != "" {
		return "", cDSN
	}
	if cDB != "" {
		if cfg.Database[cDB].Driver == "" {
			log.Printf("ERROR: db[%s] does not found", cDB)
		} else {
			return cfg.Database[cDB].Driver, cfg.Database[cDB].Dsn
		}
	}
	return "", ""
}

var argQuote = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func quotedArg(arg string) string {
	if argQuote.MatchString(arg) {
		return arg
	}
	return `"` + arg + `"`
}

func guessCompression(fileName string) string {
	dotExt := filepath.Ext(fileName)
	ext := strings.ToLower(strings.TrimLeft(dotExt, "."))
	switch ext {
	case "gz":
		return "gzip"
	case "bz2":
		return "bzip2"
	case "lz4":
		return "lz4"
	case "zst":
		return "zstd"
	case "xz":
		return "xz"
	default:
		return ""
	}
}

func compressionWriter(w io.Writer, compression string) (io.Writer, error) {
	switch strings.ToLower(compression) {
	case "gz", "gzip":
		return gzip.NewWriter(w), nil
	case "bz2", "bzip2":
		return bzip2.NewWriter(w, &bzip2.WriterConfig{})
	case "zst", "zstd":
		return zstd.NewWriter(w)
	case "lz4":
		return lz4.NewWriter(w), nil
	case "xz":
		return xz.NewWriter(w)
	default:
		return w, nil
	}
}
