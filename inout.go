package main

import (
	"compress/gzip"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Input is wrap the reader.
type Input interface {
	GetColumn(rowNum int) ([]string, error)
	GetTypes() ([]string, error)
	PreReadRow() [][]interface{}
	ReadRow([]interface{}) ([]interface{}, error)
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

func sqlFields(line string) []string {
	parsed := []string{}
	buf := ""
	var singleQuoted, doubleQuoted, backQuote bool
	for _, r := range line {
		switch r {
		case ' ', '\t', '\r', '\n', ',', ';', '=':
			if !singleQuoted && !doubleQuoted && !backQuote {
				if buf != "" {
					parsed = append(parsed, buf)
					buf = ""
				}
				if r == ',' {
					parsed = append(parsed, ",")
				}
			} else {
				buf += string(r)
			}
			continue
		case '\'':
			if !doubleQuoted && !backQuote {
				singleQuoted = !singleQuoted
			}
		case '"':
			if !singleQuoted && !backQuote {
				doubleQuoted = !doubleQuoted
			}
		case '`':
			if !singleQuoted && !doubleQuoted {
				backQuote = !backQuote
			}
		}
		buf += string(r)
	}
	parsed = append(parsed, buf)
	return parsed
}

func isSQLkey(str string) bool {
	switch strings.ToUpper(str) {
	case "WHERE", "GROUP", "HAVING", "WINDOW", "UNION", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR", "LEFT", "RIGHT", "CROSS", "INNER", "FULL", "LETERAL", "(SELECT":
		return true
	}
	return false
}

func tableList(sqlstr string) []string {
	var tableList []string
	var tableFlag, frontFlag bool
	word := sqlFields(sqlstr)
	debug.Printf("[%s]", strings.Join(word, "]["))
	for i, w := range word {
		frontFlag = false
		switch {
		case strings.ToUpper(w) == "FROM" || strings.ToUpper(w) == "JOIN":
			tableFlag = true
			frontFlag = true
		case isSQLkey(w):
			tableFlag = false
		case w == ",":
			frontFlag = true
		default:
			frontFlag = false
		}
		if n := i + 1; n < len(word) && tableFlag && frontFlag {
			if t := word[n]; len(t) > 0 {
				if t[len(t)-1] == ')' {
					t = t[:len(t)-1]
				}
				if !isSQLkey(t) {
					tableList = append(tableList, t)
				}
			}
		}
	}
	return tableList
}

func (trdsql *TRDSQL) inputFileOpen(tablename string) (io.ReadCloser, error) {
	var reader io.ReadCloser
	var err error
	r := regexp.MustCompile(`\*|\?|\[`)
	if r.MatchString(tablename) {
		reader, err = globFileOpen(tablename)
	} else {
		reader, err = tableFileOpen(tablename)
	}
	if err != nil {
		return nil, err
	}
	return reader, err
}

func (trdsql *TRDSQL) importTable(db *DDB, tablename string, sqlstr string) (string, error) {
	file, err := trdsql.inputFileOpen(tablename)
	if err != nil {
		debug.Printf("%s\n", err)
		return sqlstr, nil
	}
	defer file.Close()
	input, err := trdsql.InputNew(file, tablename)
	if err != nil {
		return sqlstr, err
	}

	if trdsql.inSkip > 0 {
		skip := make([]interface{}, 1)
		for i := 0; i < trdsql.inSkip; i++ {
			r, e := input.ReadRow(skip)
			if e != nil {
				log.Printf("ERROR: skip error %s", e)
				break
			}
			debug.Printf("Skip row:%s\n", r)
		}
	}
	rtable := db.EscapeTable(tablename)
	sqlstr = db.RewriteSQL(sqlstr, tablename, rtable)
	columnNames, err := input.GetColumn(trdsql.inPreRead)
	if err != nil {
		if err != io.EOF {
			return sqlstr, err
		}
		debug.Printf("EOF reached before argument number of rows")
	}
	columnTypes, err := input.GetTypes()
	if err != nil {
		if err != io.EOF {
			return sqlstr, err
		}
		debug.Printf("EOF reached before argument number of rows")
	}

	debug.Printf("Column Names: [%v]", strings.Join(columnNames, ","))
	debug.Printf("Column Types: [%v]", strings.Join(columnTypes, ","))
	err = db.CreateTable(rtable, columnNames, columnTypes)
	if err != nil {
		return sqlstr, err
	}
	err = db.Import(rtable, columnNames, input, trdsql.inPreRead)
	return sqlstr, err
}

// InputNew is create input reader.
func (trdsql *TRDSQL) InputNew(reader io.Reader, tablename string) (Input, error) {
	var err error
	if trdsql.inGuess {
		trdsql.inType = guessExtension(tablename)
	}
	var input Input
	switch trdsql.inType {
	case LTSV:
		input, err = trdsql.ltsvInputNew(reader)
	case JSON:
		input, err = trdsql.jsonInputNew(reader)
	case TBLN:
		input, err = trdsql.tblnInputNew(reader)
	default:
		input, err = trdsql.csvInputNew(reader)
	}
	return input, err
}

func trimQuote(filename string) string {
	if filename[0] == '`' {
		filename = strings.Replace(filename, "`", "", 2)
	}
	if filename[0] == '"' {
		filename = strings.Replace(filename, "\"", "", 2)
	}
	return filename
}

func extFileReader(filename string, reader *os.File) io.ReadCloser {
	if strings.HasSuffix(filename, ".gz") {
		z, err := gzip.NewReader(reader)
		if err != nil {
			debug.Printf("No gzip file: [%s]", filename)
			_, err := reader.Seek(0, io.SeekStart)
			if err != nil {
				return nil
			}
			return reader
		}
		debug.Printf("decompress gzip file: [%s]", filename)
		return z
	}
	return reader
}

func globFileOpen(filename string) (*io.PipeReader, error) {
	filename = trimQuote(filename)
	files, err := filepath.Glob(filename)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("No matches found: %s", filename)
	}
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer pipeWriter.Close()
		for _, file := range files {
			f, err := os.Open(file)
			debug.Printf("Open: [%s]", file)
			if err != nil {
				log.Printf("ERROR: %s:%s", file, err)
				continue
			}
			r := extFileReader(file, f)
			_, err = io.Copy(pipeWriter, r)
			if err != nil {
				log.Printf("ERROR: %s:%s", file, err)
				continue
			}
			_, err = pipeWriter.Write([]byte("\n"))
			if err != nil {
				log.Printf("ERROR: %s:%s", file, err)
				continue
			}
			err = f.Close()
			if err != nil {
				log.Printf("ERROR: %s:%s", file, err)
			}
		}
	}()
	return pipeReader, nil
}

func tableFileOpen(filename string) (io.ReadCloser, error) {
	if len(filename) == 0 || filename == "-" || strings.ToLower(filename) == "stdin" {
		return os.Stdin, nil
	}
	filename = trimQuote(filename)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	var r io.ReadCloser
	r = extFileReader(filename, file)
	return r, nil
}

// Output is database export
type Output interface {
	First([]string, []string) error
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
			log.Printf("ERROR: close:%s", err)
		}
	}()
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	types := make([]string, len(columns))
	for i, ct := range columnTypes {
		types[i] = convertType(ct.DatabaseTypeName())
	}

	err = output.First(columns, types)
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

func convertType(dbtype string) string {
	switch strings.ToLower(dbtype) {
	case "smallint", "integer", "int", "int2", "int4", "smallserial", "serial":
		return "int"
	case "bigint", "int8", "bigserial":
		return "bigint"
	case "float", "decimal", "numeric", "real", "double precision":
		return "numeric"
	case "bool":
		return "bool"
	case "timestamp", "timestamptz", "date", "time":
		return "timestamp"
	case "string", "text", "char", "varchar":
		return "text"
	default:
		return "text"
	}
}

func guessExtension(tablename string) int {
	if strings.HasSuffix(tablename, ".gz") {
		tablename = tablename[0 : len(tablename)-3]
	}
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

func valString(v interface{}) string {
	var str string
	switch t := v.(type) {
	case nil:
		str = ""
	case time.Time:
		str = t.Format(time.RFC3339)
	case []byte:
		if ok := utf8.Valid(t); ok {
			str = string(t)
		} else {
			str = `\x` + hex.EncodeToString(t)
		}
	default:
		str = fmt.Sprint(v)
		str = strings.ReplaceAll(str, "\n", "\\n")
	}
	return str
}
