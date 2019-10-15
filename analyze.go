package trdsql

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/olekukonko/tablewriter"
)

var au aurora.Aurora

// Color is enables or disables color output.
func Color(color bool) {
	au = aurora.NewAurora(color)
}

// Analyze analyzes the file and outputs the table information.
// In addition, SQL execution examples are output.
func Analyze(fileName string, command string, driver string, readOpts *ReadOpts) error {

	color := os.Getenv("NO_COLOR")
	if color == "" && runtime.GOOS != "windows" {
		Color(true)
	} else {
		Color(false)
	}
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
	fmt.Printf("The table name is %s.\n", au.Yellow(fileName))
	fmt.Printf("The file type is %s.\n", au.Red(readOpts.realFormat))
	names := make([]string, len(columnNames))
	for i := range columnNames {
		names[i] = quoted(driver, columnNames[i])
	}
	if len(names) <= 1 && readOpts.realFormat == CSV {
		fmt.Println(au.Magenta("Is the delimiter different?"))
		fmt.Println(au.Magenta("Please try again with -id \"\\t\" or -id \" \"."))
	}

	fmt.Println(au.Cyan("\nData types:"))
	typeTable := tablewriter.NewWriter(os.Stdout)
	typeTable.SetAutoFormatHeaders(false)
	typeTable.SetHeader([]string{"column name", "type"})
	for i := range columnNames {
		typeTable.Append([]string{names[i], columnTypes[i]})
	}
	typeTable.Render()

	fmt.Println(au.Cyan("\nData samples:"))
	sampleTable := tablewriter.NewWriter(os.Stdout)
	sampleTable.SetAutoFormatHeaders(false)
	sampleTable.SetHeader(names)
	results := make([]string, len(names))
	for _, row := range reader.PreReadRow() {
		for i, col := range row {
			results[i] = ValString(col)
		}
		sampleTable.Append(results)
	}
	sampleTable.Render()

	fmt.Println(au.Cyan("\nExamples:"))
	queries := examples(fileName, names, results)
	for _, query := range queries {
		fmt.Println(command, `"`+query+`"`)
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

func quoted(driver string, name string) string {
	r := regexp.MustCompile(`^[a-z0-9_]+$`)
	if r.MatchString(name) {
		return name
	}
	quote := "\\`"
	if driver == "postgres" {
		quote = `\"`
	}
	return quote + name + quote
}
