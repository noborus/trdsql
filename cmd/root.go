package cmd

import (
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/dsnet/compress/bzip2"
	"github.com/klauspost/compress/zstd"
	"github.com/noborus/trdsql"
	"github.com/pierrec/lz4"
	"github.com/spf13/cobra"
	"github.com/ulikunitz/xz"

	"github.com/spf13/viper"
)

var (
	usage      bool
	version    bool
	cfgFile    string
	completion string

	dbList    bool
	cDB       string
	cDriver   string
	cDSN      string
	guess     bool
	queryFile string
	analyze   string
	onlySQL   string
	tableName string

	inFormat    string
	inDelimiter string
	inHeader    bool
	inSkip      int
	inPreRead   int
	inJQuery    string
	inLimitRead int
	inNull      nilString

	outFormat       string
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

// connection is a driver and dsn connection configuration.
type connection struct {
	Driver string `json:"driver"`
	Dsn    string `json:"dsn"`
}

// dbConfig is a configuration for the database.
type dbConfig struct {
	Db       string                `json:"db"`
	Database map[string]connection `json:"database"`
}

var dbCfg *dbConfig

// TableQuery is a query to use instead of TABLE.
const TableQuery = "SELECT * FROM"

// Debug represents a flag for detailed output.
var Debug bool

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "trdsql",
	Short: fmt.Sprintf("%s - Execute SQL queries on CSV, LTSV, JSON, YAML and TBLN.", trdsql.AppName),
	Long:  fmt.Sprintf("%s - Execute SQL queries on CSV, LTSV, JSON, YAML and TBLN.", trdsql.AppName),

	Run: func(cmd *cobra.Command, args []string) {
		var writer io.Writer = os.Stdout
		if version {
			fmt.Fprintf(writer, "%s version %s\n", trdsql.AppName, trdsql.Version)
			return
		}
		if completion != "" {
			Completion(writer, cmd, completion)
		}

		if Debug {
			trdsql.EnableDebug()
		}

		// MultipleQueries is enabled by default.
		trdsql.EnableMultipleQueries()

		if dbList {
			printDBList(writer, dbCfg)
			return
		}

		if analyze != "" || onlySQL != "" {
			if err := analyzeSQL(writer, dbCfg, analyze, onlySQL); err != nil {
				log.Println(err)
				return
			}
		}

		if err := run(writer, dbCfg, args); err != nil {
			log.Println(err)
		}
	},
}

func Completion(writer io.Writer, cmd *cobra.Command, shell string) {
	switch completion {
	case "bash":
		cmd.GenBashCompletion(writer)
	case "zsh":
		cmd.GenZshCompletion(writer)
	case "fish":
		cmd.GenFishCompletion(writer, true)
	case "powershell":
		cmd.GenPowerShellCompletion(writer)
	default:
		fmt.Fprintf(os.Stderr, "Unknown completion: %s\n", completion)
	}
}

func run(writer io.Writer, cfg *dbConfig, args []string) error {
	driver, dsn := getDB(cfg, cDB, cDriver, cDSN)
	importer := trdsql.NewImporter(
		trdsql.InFormat(strToFormat(inFormat)),
		trdsql.InHeader(inHeader),
		trdsql.InSkip(inSkip),
		trdsql.InJQ(inJQuery),
		trdsql.InNeedNULL(inNull.valid),
		trdsql.InNULL(inNull.str),
	)

	if outFile != "" {
		w, err := os.Create(outFile)
		if err != nil {
			return err
		}
		writer = w
	}

	outFormat := strToFormat(outFormat)
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

	if cWriter, err := compressionWriter(writer, outCompression); err != nil {
		return err
	} else {
		writer = cWriter
	}
	w := trdsql.NewWriter(
		trdsql.OutDelimiter(outDelimiter),
		trdsql.OutFormat(outFormat),
		trdsql.OutQuote(outQuote),
		trdsql.OutAllQuotes(outAllQuotes),
		trdsql.OutUseCRLF(outUseCRLF),
		trdsql.OutHeader(outHeader),
		trdsql.OutNoWrap(outNoWrap),
		trdsql.OutNeedNULL(outNull.valid),
		trdsql.OutNULL(outNull.str),
		trdsql.OutStream(writer),
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

	query, err := getQuery(args, tableName, queryFile)
	if err != nil {
		return err
	}
	if err := trd.ExecContext(ctx, query); err != nil {
		return err
	}

	if wc, ok := writer.(io.Closer); ok {
		err := wc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// strToFormat returns format from flag.
func strToFormat(format string) trdsql.Format {
	switch strings.ToLower(format) {
	case "csv":
		return trdsql.CSV
	case "ltsv":
		return trdsql.LTSV
	case "json":
		return trdsql.JSON
	case "jsonl":
		return trdsql.JSONL
	case "raw":
		return trdsql.RAW
	case "md":
		return trdsql.MD
	case "at":
		return trdsql.AT
	case "yaml":
		return trdsql.YAML
	case "tbln":
		return trdsql.TBLN
	case "width":
		return trdsql.WIDTH
	default:
		return trdsql.GUESS
	}
}

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

func (v *nilString) Type() string {
	return "string"
}

func analyzeSQL(writer io.Writer, cfg *dbConfig, table string, onlySQL string) error {
	opts := trdsql.NewAnalyzeOpts()
	opts.OutStream = writer

	driver, _ := getDB(cfg, cDB, cDriver, cDSN)

	opts = quoteOpts(opts, driver)
	if onlySQL != "" {
		table = onlySQL
		opts.Detail = false
	}
	opts = optsCommand(opts, os.Args)

	if inHeader && inPreRead == 1 {
		inPreRead = 2
	}
	readOpts := trdsql.NewReadOpts(
		trdsql.InFormat(strToFormat(inFormat)),
		trdsql.InDelimiter(inDelimiter),
		trdsql.InHeader(inHeader),
		trdsql.InSkip(inSkip),
		trdsql.InPreRead(inPreRead),
		trdsql.InJQ(inJQuery),
	)
	if err := trdsql.Analyze(table, opts, readOpts); err != nil {
		return err
	}
	return nil
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is XDG_CONFIG_HOME/trdsql/config.json)")
	rootCmd.PersistentFlags().BoolVarP(&version, "version", "v", false, "display version information.")
	rootCmd.PersistentFlags().BoolVarP(&usage, "help", "h", false, "display usage information.")
	rootCmd.PersistentFlags().BoolVar(&Debug, "debug", false, "debug print.")

	rootCmd.PersistentFlags().BoolVarP(&guess, "guess", "g", true, "guess format from extension.")
	rootCmd.PersistentFlags().StringVarP(&queryFile, "query", "q", "", "read query from the specified file.")
	rootCmd.PersistentFlags().StringVarP(&analyze, "analyze", "a", "", "analyze the file and suggest SQL.")
	rootCmd.PersistentFlags().StringVarP(&onlySQL, "analyze-sql", "A", "", "analyze the file but only suggest SQL.")
	rootCmd.PersistentFlags().StringVarP(&tableName, "table", "t", "", "read table name from the specified file.")

	rootCmd.PersistentFlags().StringVar(&cDB, "db", "", "specify db name of the setting.")
	rootCmd.PersistentFlags().BoolVar(&dbList, "db-list", false, "display db information.")
	rootCmd.PersistentFlags().StringVar(&cDriver, "driver", "", "database driver.  ["+strings.Join(sql.Drivers(), "|")+"]")
	rootCmd.PersistentFlags().StringVar(&cDSN, "dsn", "", "database driver specific data source name.")

	rootCmd.PersistentFlags().StringVar(&inDelimiter, "delimiter", ",", "Input delimiter")
	rootCmd.PersistentFlags().BoolVar(&inHeader, "header", false, "the first line is interpreted as column names(CSV only).")
	rootCmd.PersistentFlags().IntVar(&inSkip, "skip", 0, "skip header row.")
	rootCmd.PersistentFlags().IntVar(&inPreRead, "pre-read", 1, "number of rows to pre-read.")
	rootCmd.PersistentFlags().IntVar(&inLimitRead, "limit", 0, "limited number of rows to read.")
	rootCmd.PersistentFlags().StringVar(&inJQuery, "jq", "", "jq expression string for input(JSON/JSONL only).")
	rootCmd.PersistentFlags().Var(&inNull, "null", "value(string) to convert to null on input.")

	rootCmd.PersistentFlags().StringVarP(&inFormat, "in", "i", "GUESS", "format for input. [CSV|LTSV|JSON|YAML|TBLN|WIDTH]")
	rootCmd.PersistentFlags().StringVar(&outDelimiter, "out-delimiter", ",", "field delimiter for output.")
	rootCmd.PersistentFlags().StringVar(&outFile, "out-file", "", "output file name.")
	rootCmd.PersistentFlags().BoolVar(&outWithoutGuess, "out-without-guess", false, "output without guessing (when using -out).")
	rootCmd.PersistentFlags().StringVar(&outQuote, "out-quote", "\"", "quote character for output.")
	rootCmd.PersistentFlags().BoolVar(&outAllQuotes, "out-all-quotes", false, "enclose all fields in quotes for output.")
	rootCmd.PersistentFlags().BoolVar(&outUseCRLF, "out-crlf", false, "use CRLF for output. End each output line with '\\r\\n' instead of '\\n'.")
	rootCmd.PersistentFlags().BoolVar(&outNoWrap, "out-nowrap", false, "do not wrap long lines(at/md only).")
	rootCmd.PersistentFlags().BoolVar(&outHeader, "out-header", false, "output column name as header.")
	rootCmd.PersistentFlags().StringVar(&outCompression, "out-compression", "", "output compression format. [gz|bz2|zstd|lz4|xz]")
	rootCmd.PersistentFlags().Var(&outNull, "out-null", "value(string) to convert from null on output.")

	rootCmd.PersistentFlags().StringVarP(&outFormat, "out", "o", "GUESS", "format for output. [CSV|LTSV|JSON|JSONL|RAW|MD|AT|YAML|TBLN]")

	rootCmd.PersistentFlags().StringVarP(&completion, "completion", "", "", "generate completion script [bash|zsh|fish|powershell]")

	_ = viper.BindPFlag("database", rootCmd.PersistentFlags().Lookup("database"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)
		xdgConfigHome := os.Getenv("XDG_CONFIG_HOME")
		// `$HOME/.config/trdsql`.
		defaultConfigPath := filepath.Join(home, ".config", "trdsql")
		if xdgConfigHome != "" {
			// `$XDG_CONFIG_HOME/trdsql`.
			defaultConfigPath = filepath.Join(xdgConfigHome, "trdsql")
		}
		viper.AddConfigPath(defaultConfigPath)
		viper.SetConfigName("config")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		// fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
	if err := viper.Unmarshal(&dbCfg); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func printDBList(w io.Writer, cfg *dbConfig) {
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
		if arg == "-a" || arg == "-A" || arg == "--analyze" || arg == "--analyze-sql" {
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

func getQuery(args []string, tableName string, queryFile string) (string, error) {
	if tableName != "" {
		var query strings.Builder
		query.WriteString(TableQuery)
		query.WriteString(" ")
		query.WriteString(tableName)
		return trimQuery(query.String()), nil
	}

	if queryFile == "" {
		return trimQuery(strings.Join(args, " ")), nil
	}

	sqlByte, err := os.ReadFile(queryFile)
	if err != nil {
		return "", err
	}
	return trimQuery(string(sqlByte)), nil
}

func getDB(cfg *dbConfig, cDB string, cDriver string, cDSN string) (string, string) {
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

func trimQuery(query string) string {
	return strings.TrimRight(strings.TrimSpace(query), ";")
}

var argQuote = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func quotedArg(arg string) string {
	if argQuote.MatchString(arg) {
		return arg
	}
	return `"` + arg + `"`
}

func outGuessFormat(fileName string) trdsql.Format {
	for {
		dotExt := filepath.Ext(fileName)
		if dotExt == "" {
			return trdsql.CSV
		}
		ext := strings.ToUpper(strings.TrimLeft(dotExt, "."))
		format := trdsql.OutputFormat(ext)
		if format != trdsql.GUESS {
			return format
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
