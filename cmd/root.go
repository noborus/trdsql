package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/noborus/trdsql"
	"github.com/spf13/cobra"

	"github.com/spf13/viper"
)

var (
	usage      bool
	version    bool
	cfgFile    string
	completion string
	dbList     bool
	cDB        string
	cDriver    string
	cDSN       string
	guess      bool
	queryFile  string
	analyze    string
	onlySQL    string
	tableName  string

	inFlag      string
	inDelimiter string
	inHeader    bool
	inSkip      int
	inPreRead   int
	inJQuery    string
	inLimitRead int
	inNull      nilString

	outFlag         string
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

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "trdsql",
	Short: fmt.Sprintf("%s - Execute SQL queries on CSV, LTSV, JSON, YAML and TBLN.", trdsql.AppName),
	Long:  fmt.Sprintf("%s - Execute SQL queries on CSV, LTSV, JSON, YAML and TBLN.", trdsql.AppName),

	Run: func(cmd *cobra.Command, args []string) {
		if version {
			fmt.Fprintf(os.Stdout, "%s version %s\n", trdsql.AppName, trdsql.Version)
			return
		}
		if completion != "" {
			Completion(cmd, completion)
		}

		if analyze != "" || onlySQL != "" {
			if err := analyzeSQL(analyze, onlySQL); err != nil {
				log.Println(err)
				return
			}
		}

		if err := run(args); err != nil {
			log.Println(err)
		}
	},
}

func Completion(cmd *cobra.Command, shell string) {
	switch completion {
	case "bash":
		cmd.GenBashCompletion(os.Stdout)
	case "zsh":
		cmd.GenZshCompletion(os.Stdout)
	case "fish":
		cmd.GenFishCompletion(os.Stdout, true)
	case "powershell":
		cmd.GenPowerShellCompletion(os.Stdout)
	default:
		fmt.Fprintf(os.Stderr, "Unknown completion: %s\n", completion)
	}
}

func run(args []string) error {
	driver := cDriver
	dsn := cDSN
	importer := trdsql.NewImporter(
		trdsql.InFormat(strToFormat(inFlag)),
		trdsql.InHeader(inHeader),
		trdsql.InSkip(inSkip),
		trdsql.InJQ(inJQuery),
		trdsql.InNeedNULL(inNull.valid),
		trdsql.InNULL(inNull.str),
	)

	var writer io.Writer
	writer = os.Stdout
	if outFile != "" {
		w, err := os.Create(outFile)
		if err != nil {
			return err
		}
		writer = w
	}

	outFormat := strToFormat(outFlag)
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

func analyzeSQL(table string, onlySQL string) error {
	opts := trdsql.NewAnalyzeOpts()
	opts.OutStream = os.Stdout
	opts = quoteOpts(opts, cDriver)
	if onlySQL != "" {
		table = onlySQL
		opts.Detail = false
	}
	opts = optsCommand(opts, os.Args)

	if inHeader && inPreRead == 1 {
		inPreRead = 2
	}
	readOpts := trdsql.NewReadOpts(
		trdsql.InFormat(strToFormat(inFlag)),
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

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.trdsql.yaml)")
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
	rootCmd.PersistentFlags().StringVar(&cDriver, "driver", "", "database driver.  [ "+strings.Join(sql.Drivers(), " | ")+" ]")
	rootCmd.PersistentFlags().StringVar(&cDSN, "dsn", "", "database driver specific data source name.")

	rootCmd.PersistentFlags().StringVar(&inDelimiter, "delimiter", ",", "Input delimiter")
	rootCmd.PersistentFlags().BoolVar(&inHeader, "header", false, "the first line is interpreted as column names(CSV only).")
	rootCmd.PersistentFlags().IntVar(&inSkip, "skip", 0, "skip header row.")
	rootCmd.PersistentFlags().IntVar(&inPreRead, "pre-read", 1, "number of rows to pre-read.")
	rootCmd.PersistentFlags().IntVar(&inLimitRead, "limit", 0, "limited number of rows to read.")
	rootCmd.PersistentFlags().StringVar(&inJQuery, "jq", "", "jq expression string for input(JSON/JSONL only).")
	rootCmd.PersistentFlags().Var(&inNull, "null", "value(string) to convert to null on input.")

	rootCmd.PersistentFlags().StringVarP(&inFlag, "in", "i", "CSV", "format for input.")

	rootCmd.PersistentFlags().StringVar(&outDelimiter, "out-delimiter", ",", "field delimiter for output.")
	rootCmd.PersistentFlags().StringVar(&outFile, "out-file", "", "output file name.")
	rootCmd.PersistentFlags().BoolVar(&outWithoutGuess, "out-without-guess", false, "output without guessing (when using -out).")
	rootCmd.PersistentFlags().StringVar(&outQuote, "out-quote", "\"", "quote character for output.")
	rootCmd.PersistentFlags().BoolVar(&outAllQuotes, "out-all-quotes", false, "enclose all fields in quotes for output.")
	rootCmd.PersistentFlags().BoolVar(&outUseCRLF, "out-crlf", false, "use CRLF for output. End each output line with '\\r\\n' instead of '\\n'.")
	rootCmd.PersistentFlags().BoolVar(&outNoWrap, "out-nowrap", false, "do not wrap long lines(at/md only).")
	rootCmd.PersistentFlags().BoolVar(&outHeader, "out-header", false, "output column name as header.")
	rootCmd.PersistentFlags().StringVar(&outCompression, "out-compression", "", "output compression format. [ gz | bz2 | zstd | lz4 | xz ]")
	rootCmd.PersistentFlags().Var(&outNull, "out-null", "value(string) to convert from null on output.")

	rootCmd.PersistentFlags().StringVarP(&outFlag, "out", "o", "", "format for output.")

	rootCmd.PersistentFlags().StringVarP(&completion, "completion", "", "", "generate completion script [bash|zsh|fish|powershell]")
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

		// Search config in home directory with name ".trdsql" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".trdsql")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}
