package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// Input is wrap the reader.
type Input interface {
	FirstRead() ([]string, error)
	FirstRowRead([]interface{}) []interface{}
	RowRead([]interface{}) ([]interface{}, error)
}

// Import is import the file written in SQL.
func (trdsql *TRDSQL) Import(db *DDB, sqlstr string) (string, error) {
	var err error
	tableList := tableList(sqlstr)
	if len(tableList) == 0 {
		// without FROM clause. ex. SELECT 1+1;
		debug.Printf("table not found\n")
		return sqlstr, nil
	}
	created := make(map[string]bool)
	for _, tablename := range tableList {
		if created[tablename] {
			debug.Printf("already created \"%s\"\n", tablename)
			continue
		}
		sqlstr, err = trdsql.importTable(db, tablename, sqlstr)
		if err != nil {
			break
		}
		created[tablename] = true
	}
	return sqlstr, err
}

// Fixes the issue with the tableList ignoring quotes in the table list
func stringSplitWithQuotes(data string) []string {
	// Split string
	r := csv.NewReader(strings.NewReader(data))
	r.Comma = ' ' // space
	out, err := r.Read()
	if err != nil {
		fmt.Println(err)
	}
	return out
}

func tableList(sqlstr string) []string {
	var tableList []string

	// Get a section of SQL that would contain table names
	tableSection := stringRegex(sqlstr, `(?is)[\r\n\s]FROM\s(.+)(?:[\r\n\s]WHERE|$)`)

	// get possible table names
	possibleTables := stringRegex(tableSection[0], `(?is)(?:"([^"]*)"|'([^']*)'|([^\s"']+))(?:\s+AS\s+(?:"[^"]+"|\S+))?`)
	//fmt.Printf("%v\n", possibleTables)

	for _, table := range possibleTables {
		if len(table) < 3 || stringRegexMatch(table, "(?i)JOIN|INNER|OUTER|LEFT|CROSS") {
			continue
		}
		if !stringRegexMatch(table, `\*`) && !fileExistsCheck(table) {
			continue
		}
		tableList = append(tableList, table)
	}
	return tableList
}

// check if file exists given path
func fileExistsCheck(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	}
	return false
}

// Simple regex: return one non-empty match per subgroup in a string array
func stringRegex(text string, reg string) []string {
	groups := regexp.MustCompile(reg).FindAllStringSubmatch(text, -1)
	var output []string
	for _, word := range groups {
		for i, w := range word {
			if i == 0 || w == "" {
				continue
			}
			output = append(output, w)
		}
	}
	return output
}

// does the text match the regex?
func stringRegexMatch(text string, reg string) bool {
	match, _ := regexp.MatchString(reg, text)
	return match
}

func (trdsql *TRDSQL) importTable(db *DDB, tablename string, sqlstr string) (string, error) {

	rtable := db.EscapeTable(tablename)
	sqlstr = db.RewriteSQL(sqlstr, tablename, rtable)
	var colNames []string

	// supports table name to be an explicit file or a glob pattern for the files
	fileCollection, err := filepath.Glob(tablename)

	// read all of the columns in all the files (each file may have a different column set)
	for _, filePath := range fileCollection {
		debug.Printf(`Reading columns from: "` + filePath + `"`)
		file, err := tableFileOpen(filePath)
		if err != nil {
			debug.Printf("%s\n", err)
			return sqlstr, nil
		}
		defer func() {
			err = file.Close()
			if err != nil {
				log.Println("ERROR:", err)
			}
		}()
		input, err := trdsql.InputNew(file, filePath)
		if err != nil {
			log.Println("ERROR:", err)
		}
		if trdsql.inSkip > 0 {
			skip := make([]interface{}, 1)
			for i := 0; i < trdsql.inSkip; i++ {
				r, _ := input.RowRead(skip)
				debug.Printf("Skip row:%s\n", r)
			}
		}

		colNew, err := input.FirstRead()
		if err != nil {
			return sqlstr, err
		}
		for _, colItemNew := range colNew {
			exists := false
			for _, colItem := range colNames {
				if colItem == colItemNew {
					exists = true
					break
				}
			}
			if !exists {
				colNames = append(colNames, colItemNew)
			}
		}
	}
	debug.Printf(`Final column names: %v`, colNames)

	// Create Table
	err = db.CreateTable(rtable, colNames)
	if err != nil {
		return sqlstr, err
	}

	// insert the data from each file
	for _, filePath := range fileCollection {
		debug.Printf(`Reading: "` + filePath + `"`)

		file, err := tableFileOpen(filePath)
		if err != nil {
			debug.Printf("%s\n", err)
			return sqlstr, nil
		}
		defer func() {
			err = file.Close()
			if err != nil {
				log.Println("ERROR:", err)
			}
		}()
		input, err := trdsql.InputNew(file, filePath)
		if err != nil {
			log.Println("ERROR:", err)
		}
		if trdsql.inSkip > 0 {
			skip := make([]interface{}, 1)
			for i := 0; i < trdsql.inSkip; i++ {
				r, _ := input.RowRead(skip)
				debug.Printf("Skip row:%s\n", r)
			}
		}
		// skip the header for each file if present
		if trdsql.inHeader {
			skip := make([]interface{}, 1)
			input.RowRead(skip)
		}
		err = db.Import(rtable, colNames, input, trdsql.inFirstRow)
	}
	return sqlstr, err
}

func (trdsql *TRDSQL) createTable(db *DDB, tablename string, sqlstr string) (string, error) {
	file, err := tableFileOpen(tablename)
	if err != nil {
		debug.Printf("%s\n", err)
		return sqlstr, nil
	}
	defer func() {
		err = file.Close()
		if err != nil {
			log.Println("ERROR:", err)
		}
	}()
	input, err := trdsql.InputNew(file, tablename)
	if err != nil {
		log.Println("ERROR:", err)
	}
	if trdsql.inSkip > 0 {
		skip := make([]interface{}, 1)
		for i := 0; i < trdsql.inSkip; i++ {
			r, _ := input.RowRead(skip)
			debug.Printf("Skip row:%s\n", r)
		}
	}
	rtable := db.EscapeTable(tablename)
	sqlstr = db.RewriteSQL(sqlstr, tablename, rtable)
	name, err := input.FirstRead()
	if err != nil {
		return sqlstr, err
	}
	err = db.CreateTable(rtable, name)
	if err != nil {
		return "", err
	}
	return "", err
}

// InputNew is create input reader.
func (trdsql *TRDSQL) InputNew(file io.Reader, tablename string) (Input, error) {
	var err error
	if trdsql.inGuess {
		trdsql.inType = guessExtension(tablename)
	}
	trdsql.inFirstRow = false
	var input Input
	switch trdsql.inType {
	case LTSV:
		trdsql.inFirstRow = true
		input, err = trdsql.ltsvInputNew(file)
	case JSON:
		trdsql.inFirstRow = true
		input, err = trdsql.jsonInputNew(file)
	default:
		trdsql.inFirstRow = !trdsql.inHeader
		input, err = trdsql.csvInputNew(file)
	}
	return input, err
}

func tableFileOpen(filename string) (*os.File, error) {
	if filename == "-" || strings.ToLower(filename) == "stdin" {
		return os.Stdin, nil
	}
	if filename[0] == '`' {
		filename = strings.Replace(filename, "`", "", 2)
	}
	if filename[0] == '"' {
		filename = strings.Replace(filename, "\"", "", 2)
	}
	return os.Open(filename)
}

// Output is database export
type Output interface {
	First([]string) error
	RowWrite([]interface{}, []string) error
	Last() error
}

// Export is execute SQL and output the result.
func (trdsql *TRDSQL) Export(db *DDB, sqlstr string, output Output) error {
	rows, err := db.Select(sqlstr)
	if err != nil {
		return err
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			log.Println("ERROR:", err)
		}
	}()
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	err = output.First(columns)
	if err != nil {
		return err
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}
		err = output.RowWrite(values, columns)
		if err != nil {
			return err
		}
	}
	return output.Last()
}

func guessExtension(tablename string) int {
	pos := strings.LastIndex(tablename, ".")
	if pos > 0 {
		ext := strings.ToLower(tablename[pos:])
		if strings.Contains(ext, ".ltsv") {
			debug.Printf("Guess file type as LTSV: [%s]", tablename)
			return LTSV
		} else if strings.Contains(ext, ".json") {
			debug.Printf("Guess file type as JSON: [%s]", tablename)
			return JSON
		}
	}
	debug.Printf("Guess file type as CSV: [%s]", tablename)
	return CSV
}

func separator(sepString string) (rune, error) {
	if sepString == "" {
		return 0, nil
	}
	sepRunes, err := strconv.Unquote(`'` + sepString + `'`)
	if err != nil {
		return ',', fmt.Errorf("Can not get separator: %s:\"%s\"", err, sepString)
	}
	sepRune := ([]rune(sepRunes))[0]
	return sepRune, err
}

func valString(v interface{}) string {
	var str string
	b, ok := v.([]byte)
	if ok {
		str = string(b)
	} else {
		if v == nil {
			str = ""
		} else {
			str = fmt.Sprint(v)
		}
	}
	return str
}
