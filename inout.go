package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

// Input is wrap the reader.
type Input interface {
	firstRead() ([]string, error)
	firstRowRead([]interface{}) []interface{}
	rowRead([]interface{}) ([]interface{}, error)
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

func tableList(sqlstr string) []string {
	var tableList []string
	word := strings.Fields(sqlstr)
	for i, w := range word {
		if element := strings.ToUpper(w); element == "FROM" || element == "JOIN" {
			if (i + 1) < len(word) {
				tableList = append(tableList, word[i+1])
			}
		}
	}
	return tableList
}

func (trdsql *TRDSQL) importTable(db *DDB, tablename string, sqlstr string) (string, error) {
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
			r, _ := input.rowRead(skip)
			debug.Printf("Skip row:%s\n", r)
		}
	}
	rtable := db.EscapeTable(tablename)
	sqlstr = db.RewriteSQL(sqlstr, tablename, rtable)
	var header []string
	header, err = input.firstRead()
	if err != nil {
		return sqlstr, err
	}
	err = db.CreateTable(rtable, header)
	if err != nil {
		return sqlstr, err
	}
	err = db.Import(rtable, header, input, trdsql.inFirstRow)
	return sqlstr, err
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
	first([]string) error
	rowWrite([]interface{}, []string) error
	last() error
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
	err = output.first(columns)
	if err != nil {
		return err
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}
		err = output.rowWrite(values, columns)
		if err != nil {
			return err
		}
	}
	return output.last()
}

func guessExtension(tablename string) int {
	pos := strings.LastIndex(tablename, ".")
	if pos > 0 {
		ext := strings.ToLower(tablename[pos:])
		if ext == ".ltsv" {
			debug.Printf("Guess file type as LTSV: [%s]", tablename)
			return LTSV
		} else if ext == ".json" {
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
		return ',', fmt.Errorf("ERROR getSeparator: %s:%s", err, sepString)
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
