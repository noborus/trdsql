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
	colorTable    = gchalk.Yellow
	colorFileType = gchalk.Red
	colorCaption  = gchalk.Cyan
	colorNotes    = gchalk.Magenta
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
	w := opts.OutStream
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
	names := quoteNames(columnNames, opts.Quote)

	columnTypes, err := reader.Types()
	if err != nil {
		return err
	}

	results := getResults(reader, len(names))

	if opts.Detail {
		fmt.Fprintf(w, "The table name is %s.\n", colorTable(tableName))
		fmt.Fprintf(w, "The file type is %s.\n", colorFileType(rOpts.realFormat.String()))
		if len(names) <= 1 && len(results) != 0 {
			additionalAdvice(w, rOpts, columnNames[0], results[0][0])
		}

		fmt.Fprintln(w, colorCaption("\nData types:"))
		typeTableRender(w, names, columnTypes)

		fmt.Fprintln(w, colorCaption("\nData samples:"))
		sampleTableRender(w, names, results)

		fmt.Fprintln(w, colorCaption("\nExamples:"))
	}

	if len(results) == 0 {
		return nil
	}
	queries := examples(tableName, names, results[0])
	for _, query := range queries {
		fmt.Fprintf(w, "%s %s\n", opts.Command, `"`+query+`"`)
	}
	return nil
}

func typeTableRender(w io.Writer, names []string, columnTypes []string) {
	typeTable := tablewriter.NewWriter(w)
	typeTable.SetAutoFormatHeaders(false)
	typeTable.SetHeader([]string{"column name", "type"})
	for i := range names {
		typeTable.Append([]string{names[i], columnTypes[i]})
	}
	typeTable.Render()
}

func sampleTableRender(w io.Writer, names []string, results [][]string) {
	sampleTable := tablewriter.NewWriter(w)
	sampleTable.SetAutoFormatHeaders(false)
	sampleTable.SetHeader(names)
	for _, row := range results {
		sampleTable.Append(row)
	}
	sampleTable.Render()
}

func additionalAdvice(w io.Writer, rOpts *ReadOpts, name string, value string) {
	switch rOpts.realFormat {
	case CSV:
		checkCSV(w, value)
	case JSON:
		checkJSON(w, rOpts.InJQuery, name)
	}
}

func checkCSV(w io.Writer, value string) {
	if value == "[" || value == "{" {
		fmt.Fprintln(w, colorNotes("Is it a JSON file?"))
		fmt.Fprintln(w, colorNotes("Please try again with -ijson."))
		return
	}
	fmt.Fprintln(w, colorNotes("Is the delimiter different?"))
	delimiter := " "
	if strings.Count(value, ";") > 1 {
		delimiter = ";"
	}
	if strings.Count(value, "\t") > 1 {
		delimiter = "\\t"
	}
	fmt.Fprintf(w, colorNotes("Please try again with -id \"%s\" or other character.\n"), delimiter)
	if strings.Contains(value, ":") {
		fmt.Fprintln(w, colorNotes("Is it a LTSV file?"))
		fmt.Fprintln(w, colorNotes("Please try again with -iltsv."))
	}
}

func checkJSON(w io.Writer, jquery string, name string) {
	fmt.Fprintln(w, colorNotes("Is it for internal objects?"))
	jq := "." + name
	if jquery != "" {
		jq = jquery + jq
	}
	fmt.Fprintf(w, colorNotes("Please try again with -ijq \"%s\".\n"), jq)
}

func quoteNames(names []string, quote string) []string {
	qnames := make([]string, len(names))
	for i := range names {
		qnames[i] = quoted(names[i], quote)
	}
	return qnames
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

func getResults(reader Reader, colNum int) [][]string {
	results := make([][]string, 0)
	for _, row := range reader.PreReadRow() {
		resultRow := make([]string, colNum)
		for j, col := range row {
			resultRow[j] = ValString(col)
		}
		results = append(results, resultRow)
	}
	return results
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
