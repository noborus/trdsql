package trdsql

import (
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/jwalton/gchalk"
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
	// OutStream is the output destination.
	OutStream io.Writer
}

// Defined to wrap string styling.
var (
	tableColor    = gchalk.Yellow
	fileTypeColor = gchalk.Red
	captionColor  = gchalk.Cyan
	notesColor    = gchalk.Magenta
)

// NewAnalyzeOpts returns AnalyzeOpts.
func NewAnalyzeOpts() *AnalyzeOpts {
	return &AnalyzeOpts{
		Command:   AppName,
		Quote:     "\\`",
		Detail:    true,
		OutStream: os.Stdout,
	}
}

// Analyze analyzes the file and outputs the table information.
// In addition, SQL execution examples are output.
func Analyze(fileName string, opts *AnalyzeOpts, readOpts *ReadOpts) error {
	rOpts, fileName := GuessOpts(readOpts, fileName)
	file, err := importFileOpen(fileName)
	if err != nil {
		return err
	}
	tableName := fileName
	if rOpts.InJQuery != "" {
		tableName = fileName + "::" + rOpts.InJQuery
	}

	defer func() {
		if deferr := file.Close(); deferr != nil {
			log.Printf("file close:%s", deferr)
		}
	}()

	reader, err := NewReader(file, rOpts)
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
		fmt.Fprintf(opts.OutStream, "The table name is %s.\n", tableColor(tableName))
		fmt.Fprintf(opts.OutStream, "The file type is %s.\n", fileTypeColor(rOpts.realFormat.String()))
		if len(names) <= 1 {
			additionalAdvice(opts, rOpts, columnNames[0], results[0][0])
		}
		fmt.Fprintln(opts.OutStream, captionColor("\nData types:"))
		typeTable.Render()
		fmt.Fprintln(opts.OutStream, captionColor("\nData samples:"))
		sampleTable.Render()
		fmt.Fprintln(opts.OutStream, captionColor("\nExamples:"))
	}

	if len(results) == 0 {
		return nil
	}

	queries := examples(tableName, names, results[0])
	for _, query := range queries {
		fmt.Fprintf(opts.OutStream, "%s %s\n", opts.Command, `"`+query+`"`)
	}
	return nil
}

func additionalAdvice(opts *AnalyzeOpts, rOpts *ReadOpts, name string, result string) {
	switch rOpts.realFormat {
	case CSV:
		if result == "[" || result == "{" {
			fmt.Fprintln(opts.OutStream, notesColor("Is it a JSON file?"))
			fmt.Fprintln(opts.OutStream, notesColor("Please try again with -ijson."))
			return
		}
		fmt.Fprintln(opts.OutStream, notesColor("Is the delimiter different?"))
		delimiter := " "
		if strings.Count(result, ";") > 1 {
			delimiter = ";"
		}
		if strings.Count(result, "\t") > 1 {
			delimiter = "\\t"
		}
		fmt.Fprintf(opts.OutStream, notesColor("Please try again with -id \"%s\" or other character.\n"), delimiter)
		if strings.Contains(result, ":") {
			fmt.Fprintln(opts.OutStream, notesColor("If it is an ltsv file, please try again with -iltsv."))
		}
	case JSON:
		fmt.Fprintln(opts.OutStream, notesColor("Is it for internal objects?"))
		jq := "." + name
		if rOpts.InJQuery != "" {
			jq = rOpts.InJQuery + jq
		}
		fmt.Fprintf(opts.OutStream, notesColor("Please try again with -ijq \"%s\".\n"), jq)
	}
}

func examples(tableName string, names []string, results []string) []string {
	queries := []string{
		// #nosec G201
		fmt.Sprintf("SELECT %s FROM %s", strings.Join(names, ", "), tableName),
		// #nosec G201
		fmt.Sprintf("SELECT %s FROM %s WHERE %s = '%s'", strings.Join(names, ", "), tableName, names[0], results[0]),
		// #nosec G201
		fmt.Sprintf("SELECT %s, count(%s) FROM %s GROUP BY %s", names[0], names[0], tableName, names[0]),
		// #nosec G201
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
