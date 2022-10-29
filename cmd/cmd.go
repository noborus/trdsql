package cmd

import (
	"compress/gzip"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/dsnet/compress/bzip2"
	"github.com/jwalton/gchalk"
	"github.com/klauspost/compress/zstd"
	"github.com/noborus/trdsql"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

// Cli wraps stdout and error output specification.
type Cli struct {
	// OutStream is the output destination.
	OutStream io.Writer

	// ErrStream is the error output destination.
	ErrStream io.Writer
}

// Debug represents a flag for detailed output.
var Debug bool

// The nilString structure represents a string
// that distinguishes between empty strings and nil.
type nilString struct {
	str   string
	valid bool
}

// String returns a string.
// nilString fills the flag#value interface.
func (v *nilString) String() string {
	return v.str
}

// Set sets the string with the valid flag set to true.
// nilString fills the flag#value interface.
func (v *nilString) Set(s string) error {
	v.str = s
	v.valid = true
	return nil
}

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
		inJQuery    string
		inLimitRead int
		inNull      nilString

		outFlag         outputFlag
		outFile         string
		outWithoutGuess bool
		outDelimiter    string
		outQuote        string
		outCompression  string
		outAllQuotes    bool
		outUseCRLF      bool
		outHeader       bool
		outNoWrap       bool
		outNull         nilString
	)

	flags := flag.NewFlagSet(trdsql.AppName, flag.ExitOnError)

	flags.SetOutput(cli.ErrStream)
	log.SetOutput(cli.ErrStream)

	flags.Usage = func() { Usage(flags) }
	flags.StringVar(&config, "config", config, "configuration file location.")
	flags.StringVar(&cDB, "db", "", "specify db name of the setting.")
	flags.BoolVar(&dbList, "dblist", false, "display db information.")
	flags.StringVar(&cDriver, "driver", "", "database driver.  [ "+strings.Join(sql.Drivers(), " | ")+" ]")
	flags.StringVar(&cDSN, "dsn", "", "database driver specific data source name.")
	flags.BoolVar(&guess, "ig", true, "guess format from extension.")
	flags.StringVar(&queryFile, "q", "", "read query from the specified file.")
	flags.StringVar(&analyze, "a", "", "analyze the file and suggest SQL.")
	flags.StringVar(&onlySQL, "A", "", "analyze the file but only suggest SQL.")
	flags.BoolVar(&usage, "help", false, "display usage information.")
	flags.BoolVar(&version, "version", false, "display version information.")
	flags.BoolVar(&Debug, "debug", false, "debug print.")

	flags.StringVar(&inDelimiter, "id", ",", "field delimiter for input.")
	flags.BoolVar(&inHeader, "ih", false, "the first line is interpreted as column names(CSV only).")
	flags.IntVar(&inSkip, "is", 0, "skip header row.")
	flags.IntVar(&inPreRead, "ir", 1, "number of rows to preread.")
	flags.IntVar(&inLimitRead, "ilr", 0, "limited number of rows to read.")
	flags.StringVar(&inJQuery, "ijq", "", "jq expression string for input(JSON/JSONL only).")
	flags.Var(&inNull, "inull", "value(string) to convert to null on input.")

	flags.BoolVar(&inFlag.CSV, "icsv", false, "CSV format for input.")
	flags.BoolVar(&inFlag.LTSV, "iltsv", false, "LTSV format for input.")
	flags.BoolVar(&inFlag.JSON, "ijson", false, "JSON format for input.")
	flags.BoolVar(&inFlag.TBLN, "itbln", false, "TBLN format for input.")

	flags.StringVar(&outFile, "out", "", "output file name.")
	flags.BoolVar(&outWithoutGuess, "out-without-guess", false, "output without guessing (when using -out).")
	flags.StringVar(&outDelimiter, "od", ",", "field delimiter for output.")
	flags.StringVar(&outQuote, "oq", "\"", "quote character for output.")
	flags.BoolVar(&outAllQuotes, "oaq", false, "enclose all fields in quotes for output.")
	flags.BoolVar(&outUseCRLF, "ocrlf", false, "use CRLF for output. End each output line with '\\r\\n' instead of '\\n'.")
	flags.BoolVar(&outNoWrap, "onowrap", false, "do not wrap long lines(at/md only).")
	flags.BoolVar(&outHeader, "oh", false, "output column name as header.")
	flags.StringVar(&outCompression, "oz", "", "output compression format. [ gz | bz2 | zstd | lz4 | xz ]")
	flags.Var(&outNull, "onull", "value(string) to convert from null on output.")

	flags.BoolVar(&outFlag.CSV, "ocsv", false, "CSV format for output.")
	flags.BoolVar(&outFlag.LTSV, "oltsv", false, "LTSV format for output.")
	flags.BoolVar(&outFlag.AT, "oat", false, "ASCII Table format for output.")
	flags.BoolVar(&outFlag.MD, "omd", false, "Markdown format for output.")
	flags.BoolVar(&outFlag.VF, "ovf", false, "Vertical format for output.")
	flags.BoolVar(&outFlag.RAW, "oraw", false, "Raw format for output.")
	flags.BoolVar(&outFlag.JSON, "ojson", false, "JSON format for output.")
	flags.BoolVar(&outFlag.TBLN, "otbln", false, "TBLN format for output.")
	flags.BoolVar(&outFlag.JSONL, "ojsonl", false, "JSON lines format for output.")

	if err := flags.Parse(args[1:]); err != nil {
		log.Printf("ERROR: %s", err)
		return 1
	}

	if version {
		fmt.Fprintf(cli.OutStream, "%s version %s\n", trdsql.AppName, trdsql.Version)
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
		printDBList(cli.OutStream, cfg)
		return 0
	}

	driver, dsn := getDB(cfg, cDB, cDriver, cDSN)

	if analyze != "" || onlySQL != "" {
		opts := trdsql.NewAnalyzeOpts()
		opts.OutStream = cli.OutStream
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
			trdsql.InPreRead(inPreRead),
			trdsql.InJQ(inJQuery),
		)
		if err = trdsql.Analyze(analyze, opts, readOpts); err != nil {
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
		Usage(flags)
		return 2
	}

	preRead := inPreRead
	limitRead := false
	if inLimitRead > 0 {
		limitRead = true
		preRead = inLimitRead
		if inSkip > 0 {
			preRead += inSkip
		}
		if inHeader {
			preRead++
		}
	}

	importer := trdsql.NewImporter(
		trdsql.InFormat(inputFormat(inFlag)),
		trdsql.InDelimiter(inDelimiter),
		trdsql.InHeader(inHeader),
		trdsql.InSkip(inSkip),
		trdsql.InPreRead(preRead),
		trdsql.InLimitRead(limitRead),
		trdsql.InJQ(inJQuery),
		trdsql.InNeedNULL(inNull.valid),
		trdsql.InNULL(inNull.str),
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
			outFormat = outGuessFormat(outFile)
		}
	}

	if outCompression == "" && !outWithoutGuess {
		outCompression = outGuessCompression(outFile)
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
		trdsql.OutNoWrap(outNoWrap),
		trdsql.OutNeedNULL(outNull.valid),
		trdsql.OutNULL(outNull.str),
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

	ctx := context.Background()

	if err = trd.ExecContext(ctx, query); err != nil {
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

// Usage is outputs usage information.
func Usage(flags *flag.FlagSet) {
	bold := gchalk.Bold
	fmt.Fprintf(flags.Output(), "%s - Execute SQL queries on CSV, LTSV, JSON and TBLN.\n\n", trdsql.AppName)
	fmt.Fprintf(flags.Output(), "%s\n", bold("Usage"))
	fmt.Fprintf(flags.Output(), "\t%s [OPTIONS] [SQL(SELECT...)]\n\n", trdsql.AppName)

	global := []string{}
	input := []string{}
	inputF := []string{}
	output := []string{}
	outputF := []string{}
	flags.VisitAll(func(flag *flag.Flag) {
		switch flag.Name[0] {
		case 'i':
			if isInFormat(flag.Name) {
				inputF = append(inputF, usageFlag(flag))
			} else {
				input = append(input, usageFlag(flag))
			}
		case 'o':
			if isOutFormat(flag.Name) {
				outputF = append(outputF, usageFlag(flag))
			} else {
				output = append(output, usageFlag(flag))
			}
		default:
			global = append(global, usageFlag(flag))
		}
	})
	fmt.Fprintf(flags.Output(), "%s\n", bold("Options:"))
	for _, u := range global {
		fmt.Fprint(flags.Output(), u, "\n")
	}

	fmt.Fprintf(flags.Output(), "\n%s\n", bold("Input Formats:"))
	for _, u := range inputF {
		fmt.Fprint(flags.Output(), u, "\n")
	}
	fmt.Fprintf(flags.Output(), "\n%s\n", bold("Input options:"))
	for _, u := range input {
		fmt.Fprint(flags.Output(), u, "\n")
	}

	fmt.Fprintf(flags.Output(), "\n%s\n", bold("Output Formats:"))
	for _, u := range outputF {
		fmt.Fprint(flags.Output(), u, "\n")
	}
	fmt.Fprintf(flags.Output(), "\n%s\n", bold("Output options:"))
	for _, u := range output {
		fmt.Fprint(flags.Output(), u, "\n")
	}
	fmt.Fprintf(flags.Output(), "\n%s\n", bold("Examples:"))
	fmt.Fprintf(flags.Output(), "  $ trdsql \"SELECT c1,c2 FROM test.csv\"\n")
	fmt.Fprintf(flags.Output(), "  $ trdsql -oltsv \"SELECT c1,c2 FROM test.json::items\"\n")
	fmt.Fprintf(flags.Output(), "  $ cat test.csv | trdsql -i csv -oltsv \"SELECT c1,c2 FROM -\"\n")
}

func usageFlag(f *flag.Flag) string {
	vType, usage := flag.UnquoteUsage(f)
	name := f.Name
	if vType != "" {
		name += " " + vType
	}
	s := fmt.Sprintf("  -%-18s %s", name, usage)
	if f.DefValue == "0" || f.DefValue == "1" {
		s += fmt.Sprintf(" (default %v)", f.DefValue)
	} else if f.DefValue != "" && f.DefValue != "false" {
		s += fmt.Sprintf(" (default %q)", f.DefValue)
	}
	return s
}

func printDBList(w io.Writer, cfg *config) {
	for od, odb := range cfg.Database {
		fmt.Fprintf(w, "%s:%s\n", od, odb.Driver)
	}
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
		if arg == "-ijq" {
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

	sqlByte, err := os.ReadFile(fileName)
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

// inputFlag represents the format of the input.
type inputFlag struct {
	CSV  bool
	LTSV bool
	JSON bool
	TBLN bool
}

// inputFormat returns format from flag.
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

func isInFormat(name string) bool {
	switch name {
	case "ig", "icsv", "iltsv", "ijson", "itbln":
		return true
	}
	return false
}

// outputFlag represents the format of the output.
type outputFlag struct {
	CSV   bool
	LTSV  bool
	JSON  bool
	JSONL bool
	TBLN  bool
	AT    bool
	MD    bool
	VF    bool
	RAW   bool
}

// outFormat returns format from flag.
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

func isOutFormat(name string) bool {
	switch name {
	case "ocsv", "oltsv", "ojson", "ojsonl", "otbln", "oat", "omd", "ovf", "oraw":
		return true
	}
	return false
}

func outGuessFormat(fileName string) trdsql.Format {
	for {
		dotExt := filepath.Ext(fileName)
		if dotExt == "" {
			return trdsql.CSV
		}
		ext := strings.ToUpper(strings.TrimLeft(dotExt, "."))
		switch ext {
		case "CSV":
			return trdsql.CSV
		case "LTSV":
			return trdsql.LTSV
		case "JSON":
			return trdsql.JSON
		case "TBLN":
			return trdsql.TBLN
		case "RAW":
			return trdsql.RAW
		case "MD":
			return trdsql.MD
		case "AT":
			return trdsql.AT
		case "VF":
			return trdsql.VF
		case "JSONL":
			return trdsql.JSONL
		}
		fileName = fileName[0 : len(fileName)-len(dotExt)]
	}
}

func outGuessCompression(fileName string) string {
	dotExt := filepath.Ext(fileName)
	ext := strings.ToLower(strings.TrimLeft(dotExt, "."))
	switch ext {
	case "gz":
		return "gzip"
	case "bz2":
		return "bzip2"
	case "zst":
		return "zstd"
	case "lz4":
		return "lz4"
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
