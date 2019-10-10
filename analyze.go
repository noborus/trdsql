package trdsql

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/olekukonko/tablewriter"
)

// Analyze analyzes the file and outputs the table information.
// In addition, SQL execution examples are output.
func Analyze(driver string, fileName string, readOpts *ReadOpts) error {
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
	fmt.Printf("The table name is %s.\n", aurora.Yellow(fileName))
	fmt.Printf("The file type is %s.\n", aurora.Red(readOpts.realFormat))
	names := make([]string, len(columnNames))
	for i := range columnNames {
		names[i] = quoted(driver, columnNames[i])
	}
	if len(names) <= 1 {
		fmt.Println(aurora.Magenta("Is the delimiter different?"))
		fmt.Println(aurora.Magenta("Please try again with -id \"\\t\" or -id \" \"."))
	}

	fmt.Println(aurora.Cyan("\nData types:"))
	typeTable := tablewriter.NewWriter(os.Stdout)
	typeTable.SetAutoFormatHeaders(false)
	typeTable.SetHeader([]string{"column name", "type"})
	for i := range columnNames {
		typeTable.Append([]string{names[i], columnTypes[i]})
	}
	typeTable.Render()

	fmt.Println(aurora.Cyan("\nData samples:"))
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

	fmt.Println(aurora.Cyan("\nExamples:"))
	command := "trdsql"
	switch readOpts.InFormat {
	case CSV:
		command += " -icsv"
	case LTSV:
		command += " -iltsv"
	case JSON:
		command += " -ijson"
	case TBLN:
		command += " -itbln"
	}
	if readOpts.realFormat == CSV || readOpts.realFormat == RAW {
		command += ` -id "` + readOpts.InDelimiter + `"`
		if readOpts.InHeader {
			command += " -ih"
		}
	}
	if readOpts.InSkip > 0 {
		command += fmt.Sprintf(" -is %d", readOpts.InSkip)
	}
	if readOpts.InPreRead > 1 {
		command += fmt.Sprintf(" -ir %d", readOpts.InPreRead)
	}
	fmt.Printf("%s \"SELECT %s FROM %s\"\n", command, strings.Join(names, ", "), fileName)
	fmt.Printf("%s \"SELECT %s FROM %s WHERE %s = '%s'\"\n", command, strings.Join(names, ", "), fileName, names[0], results[0])
	fmt.Printf("%s \"SELECT %s,count(%s) FROM %s GROUP BY %s\"\n", command, names[0], names[0], fileName, names[0])
	fmt.Printf("%s \"SELECT %s FROM %s ORDER BY %s LIMIT 10\"\n", command, strings.Join(names, ", "), fileName, names[0])
	return nil
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
