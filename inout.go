package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// Input format
const (
	CSV = iota
	LTSV
	JSON
)

// Input is database import
type Input interface {
	firstRead() ([]string, error)
	firstRow([]interface{}) []interface{}
	rowRead([]interface{}) ([]interface{}, error)
}

func (trdsql *TRDSQL) dbimport(db *DDB, sqlstr string) (string, error) {
	var err error
	tablenames := sqlparse(sqlstr)
	if len(tablenames) == 0 {
		// without FROM clause. ex. SELECT 1+1;
		debug.Printf("table not found\n")
	}
	for _, tablename := range tablenames {
		sqlstr, err = trdsql.importTable(db, tablename, sqlstr)
		if err != nil {
			debug.Printf("%s:%s", err, tablename)
			err = nil
			continue
		}
	}
	return sqlstr, err
}

func (trdsql *TRDSQL) importTable(db *DDB, tablename string, sqlstr string) (string, error) {
	input, err := trdsql.fileInput(tablename)
	if err != nil {
		return sqlstr, err
	}
	skip := make([]interface{}, 1)
	for i := 0; i < trdsql.iskip; i++ {
		r, _ := input.rowRead(skip)
		debug.Printf("Skip row:%s\n", r)
	}
	rtable := db.escapetable(tablename)
	sqlstr = db.rewrite(sqlstr, tablename, rtable)
	var header []string
	header, err = input.firstRead()
	if err != nil {
		return sqlstr, err
	}
	err = db.Create(rtable, header)
	if err != nil {
		return sqlstr, err
	}
	err = db.InsertPrepare(rtable, header)
	if err != nil {
		return sqlstr, err
	}
	err = trdsql.importData(db, input, len(header))
	return sqlstr, err
}

func (trdsql *TRDSQL) importData(db *DDB, input Input, clen int) error {
	list := make([]interface{}, clen)
	if trdsql.ifrow {
		list = input.firstRow(list)
		rowImport(db.stmt, list)
	}

	var err error
	for {
		list, err = input.rowRead(list)
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("ERROR Read: %s", err)
		}
		rowImport(db.stmt, list)
	}
	db.stmtclose()
	return nil
}

func (trdsql *TRDSQL) fileInput(tablename string) (Input, error) {
	file, err := tFileOpen(tablename)
	if err != nil {
		return nil, err
	}

	itype := CSV
	if trdsql.iltsv {
		itype = LTSV
	} else if trdsql.ijson {
		itype = JSON
	} else if trdsql.iguess {
		itype = guessExtension(tablename)
	}

	trdsql.ifrow = false
	var input Input
	switch itype {
	case LTSV:
		trdsql.ifrow = true
		input, err = trdsql.ltsvInputNew(file)
	case JSON:
		trdsql.ifrow = true
		input, err = trdsql.jsonInputNew(file)
	default:
		trdsql.ifrow = !trdsql.ihead
		input, err = trdsql.csvInputNew(file)
	}
	return input, err
}

// Output is database export
type Output interface {
	first([]string) error
	rowWrite([]interface{}, []string) error
	last()
}

func (trdsql *TRDSQL) dbexport(db *DDB, sqlstr string, output Output) error {
	rows, err := db.Select(sqlstr)
	if err != nil {
		return err
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	defer rows.Close()
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
		output.rowWrite(values, columns)
	}
	output.last()
	return nil
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

func getSeparator(sepString string) (rune, error) {
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

func tFileOpen(filename string) (*os.File, error) {
	if filename == "-" {
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
