package trdsql

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	. "github.com/logrusorgru/aurora"
	"github.com/olekukonko/tablewriter"
)

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
	fmt.Printf("The table name is %s.\n", Yellow(fileName))
	fmt.Printf("The file type is %s.\n", Red(readOpts.realFormat))
	var nLen int
	names := make([]string, len(columnNames))
	for i := range columnNames {
		names[i] = quoted(driver, columnNames[i])
		l := len(names[i])
		if nLen < l {
			nLen = l
		}
	}
	if len(names) <= 1 {
		fmt.Println(Magenta("Is the delimiter different?"))
		fmt.Println(Magenta("Please try again with -id \"\\t\" or -id \" \"."))
	}
	fmt.Println(Cyan("\nData types:"))
	colw := tablewriter.NewWriter(os.Stdout)
	colw.SetAutoFormatHeaders(false)
	colw.SetHeader([]string{"column name", "type"})
	for i := range columnNames {
		colw.Append([]string{names[i], columnTypes[i]})
	}
	colw.Render()

	fmt.Println(Cyan("\nData samples:"))
	prew := tablewriter.NewWriter(os.Stdout)
	prew.SetAutoFormatHeaders(false)
	prew.SetHeader(names)
	results := make([]string, len(names))
	for _, row := range reader.PreReadRow() {
		for i, col := range row {
			results[i] = ValString(col)
		}
		prew.Append(results)
	}
	prew.Render()

	fmt.Println(Cyan("\nExamples:"))
	args := make([]string, len(os.Args))
	for i, arg := range os.Args {
		if arg == fileName {
			continue
		}
		if arg == "-a" {
			break
		}
		if i == 0 || arg[0] == '-' {
			args[i] = arg
		} else {
			args[i] = `"` + arg + `"`
		}
	}
	command := strings.Join(args, " ")
	fmt.Printf("%s \"SELECT %s FROM %s\"\n", command, strings.Join(names, ", "), fileName)
	fmt.Printf("%s \"SELECT %s FROM %s WHERE %s = '%s'\"\n", command, strings.Join(names, ", "), fileName, names[0], results[0])
	fmt.Printf("%s \"SELECT %s,count(*) FROM %s GROUP BY %s\"\n", command, names[0], fileName, names[0])
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
