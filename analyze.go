package trdsql

import (
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/olekukonko/tablewriter"
)

// AnalyzeOpts represents the options for the operation of Analyze.
type AnalyzeOpts struct {
	// Command is string of the execution command.
	Command string
	// Quote is the quote character(s) that varies depending on the sql driver.
	Quote string
	// Detail is outputs detailed information.
	Detail bool
	// Color is a bool value for enabling color.
	Color bool
	// OutStream is the output destination.
	OutStream io.Writer
}

// NewAnalyzeOpts returns AnalyzeOpts.
func NewAnalyzeOpts() *AnalyzeOpts {
	return &AnalyzeOpts{
		Command:   AppName,
		Quote:     "\\`",
		Color:     true,
		Detail:    true,
		OutStream: os.Stdout,
	}
}

// Analyze analyzes the file and outputs the table information.
// In addition, SQL execution examples are output.
func Analyze(fileName string, opts *AnalyzeOpts, readOpts *ReadOpts) error {
	au := aurora.NewAurora(opts.Color)
	file, err := importFileOpen(fileName)
	if err != nil {
		return err
	}
	defer func() {
		err = file.Close()
		if err != nil {
			log.Printf("file close:%s", err)
		}
	}()

	readOpts = realFormat(fileName, readOpts)
	reader, err := NewReader(file, readOpts)
	if err != nil {
		return err
	}
	columnNames, err := reader.Names()
	if err != nil {
		return err
	}
	columnTypes, err := reader.Types()
	if err != nil {
		return err
	}
	names := make([]string, len(columnNames))
	for i := range columnNames {
		names[i] = quoted(columnNames[i], opts.Quote)
	}
	results := make([][]string, 0)
	for _, row := range reader.PreReadRow() {
		resultRow := make([]string, len(names))
		for j, col := range row {
			resultRow[j] = ValString(col)
		}
		results = append(results, resultRow)
	}
	typeTable := tablewriter.NewWriter(opts.OutStream)
	typeTable.SetAutoFormatHeaders(false)
	typeTable.SetHeader([]string{"column name", "type"})
	for i := range columnNames {
		typeTable.Append([]string{names[i], columnTypes[i]})
	}
	sampleTable := tablewriter.NewWriter(opts.OutStream)
	sampleTable.SetAutoFormatHeaders(false)
	sampleTable.SetHeader(names)
	for _, row := range results {
		sampleTable.Append(row)
	}
	if opts.Detail {
		fmt.Fprintf(opts.OutStream, "The table name is %s.\n", au.Yellow(fileName))
		fmt.Fprintf(opts.OutStream, "The file type is %s.\n", au.Red(readOpts.realFormat))
		if len(names) <= 1 && readOpts.realFormat == CSV {
			fmt.Fprintln(opts.OutStream, au.Magenta("Is the delimiter different?"))
			fmt.Fprintln(opts.OutStream, au.Magenta(`Please try again with -id "\t" or -id " ".`))
		}
		fmt.Fprintln(opts.OutStream, au.Cyan("\nData types:"))
		typeTable.Render()
		fmt.Fprintln(opts.OutStream, au.Cyan("\nData samples:"))
		sampleTable.Render()
		fmt.Fprintln(opts.OutStream, au.Cyan("\nExamples:"))
	}
	queries := examples(fileName, names, results[0])
	for _, query := range queries {
		fmt.Fprintf(opts.OutStream, "%s %s\n", opts.Command, `"`+query+`"`)
	}
	return nil
}

func examples(tableName string, names []string, results []string) []string {
	queries := []string{
		fmt.Sprintf("SELECT %s FROM %s", strings.Join(names, ", "), tableName),
		fmt.Sprintf("SELECT %s FROM %s WHERE %s = '%s'", strings.Join(names, ", "), tableName, names[0], results[0]),
		fmt.Sprintf("SELECT %s, count(%s) FROM %s GROUP BY %s", names[0], names[0], tableName, names[0]),
		fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT 10", strings.Join(names, ", "), tableName, names[0]),
	}
	return queries
}

var noQuoteRegexp = regexp.MustCompile(`^[a-z0-9_]+$`)

func quoted(name string, quote string) string {
	if noQuoteRegexp.MatchString(name) {
		_, exist := keywords[name]
		if !exist {
			return name
		}
	}
	return quote + name + quote
}
