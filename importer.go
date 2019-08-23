package trdsql

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Importer is the interface import data into the database.
// Importer parses sql query to decide which file to Import.
// Therefore, the reader does not receive it directly.
type Importer interface {
	Import(db *DB, query string) (string, error)
}

// ReadFormat represents a structure that satisfies the Importer.
type ReadFormat struct {
	*ReadOpts
}

// NewImporter returns trdsql default Importer.
// The argument is an option of Functional Option Pattern.
//
// usage:
//		trdsql.NewImporter(
//			trdsql.InFormat(trdsql.CSV),
//			trdsql.InHeader(true),
//			trdsql.InDelimiter(";"),
//		)
func NewImporter(options ...ReadOpt) *ReadFormat {
	readOpts := NewReadOpts(options...)
	return &ReadFormat{
		ReadOpts: readOpts,
	}
}

// DefaultDBType is default type.
const DefaultDBType = "text"

// Import is parses the SQL statement and imports one or more tables.
// Import is called from Exec.
// Return the rewritten SQL and error.
// No error is returned if there is no table to import.
func (i *ReadFormat) Import(db *DB, query string) (string, error) {
	tables := TableNames(query)
	if len(tables) == 0 {
		// without FROM clause. ex. SELECT 1+1;
		debug.Printf("table not found\n")
		return query, nil
	}
	created := make(map[string]bool)
	for _, fileName := range tables {
		if created[fileName] {
			debug.Printf("already created \"%s\"\n", fileName)
			continue
		}
		tableName, err := ImportFile(db, fileName, i.ReadOpts)
		if err != nil {
			return query, err
		}
		if tableName != "" {
			query = db.RewriteSQL(query, fileName, tableName)
			debug.Printf("escaped [%s] -> [%s]\n", fileName, tableName)
		}
		created[fileName] = true
	}

	return query, nil
}

// TableNames returns slices of table names
// that may be tables by a simple SQL parser
// from the query string of the argument.
func TableNames(query string) []string {
	var tables []string
	var tableFlag, frontFlag bool
	word := sqlFields(query)
	debug.Printf("[%s]", strings.Join(word, "]["))
	for i, w := range word {
		frontFlag = false
		switch {
		case strings.ToUpper(w) == "FROM" || strings.ToUpper(w) == "JOIN":
			tableFlag = true
			frontFlag = true
		case isSQLKeyWords(w):
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
				if !isSQLKeyWords(t) {
					tables = append(tables, t)
				}
			}
		}
	}
	return tables
}

func sqlFields(query string) []string {
	parsed := []string{}
	buf := ""
	var singleQuoted, doubleQuoted, backQuote bool
	for _, r := range query {
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

func isSQLKeyWords(str string) bool {
	switch strings.ToUpper(str) {
	case "WHERE", "GROUP", "HAVING", "WINDOW", "UNION", "ORDER", "LIMIT", "OFFSET", "FETCH",
		"FOR", "LEFT", "RIGHT", "CROSS", "INNER", "FULL", "LETERAL", "(SELECT":
		return true
	}
	return false
}

// ImportFile is imports a file.
// Return the escaped table name and error.
// Do not import if file not found (no error).
// Wildcards can be passed as fileName.
func ImportFile(db *DB, fileName string, readOpts *ReadOpts) (string, error) {
	file, err := importFileOpen(fileName)
	if err != nil {
		debug.Printf("%s\n", err)
		return "", nil
	}
	defer func() {
		err = file.Close()
		if err != nil {
			log.Printf("file close:%s", err)
		}
	}()
	if readOpts.InFormat == GUESS {
		readOpts.realFormat = guessExtension(fileName)
	} else {
		readOpts.realFormat = readOpts.InFormat
	}
	reader, err := NewReader(file, readOpts)
	if err != nil {
		return "", err
	}

	tableName := db.EscapeName(fileName)
	columnNames, err := reader.Names()
	if err != nil {
		if err != io.EOF {
			return tableName, err
		}
		debug.Printf("EOF reached before argument number of rows")
	}
	columnTypes, err := reader.Types()

	if err != nil {
		if err != io.EOF {
			return tableName, err
		}
		debug.Printf("EOF reached before argument number of rows")
	}
	debug.Printf("Column Names: [%v]", strings.Join(columnNames, ","))
	debug.Printf("Column Types: [%v]", strings.Join(columnTypes, ","))

	err = db.CreateTable(tableName, columnNames, columnTypes, readOpts.IsTemporary)
	if err != nil {
		return tableName, err
	}
	err = db.Import(tableName, columnNames, reader)
	return tableName, err
}

func guessExtension(tableName string) Format {
	if strings.HasSuffix(tableName, ".gz") {
		tableName = tableName[0 : len(tableName)-3]
	}
	pos := strings.LastIndex(tableName, ".")
	if pos <= 0 {
		debug.Printf("Set in CSV because the extension is unknown: [%s]", tableName)
		return CSV
	}
	ext := strings.ToUpper(tableName[pos+1:])
	switch ext {
	case "CSV":
		debug.Printf("Guess file type as CSV: [%s]", tableName)
		return CSV
	case "LTSV":
		debug.Printf("Guess file type as LTSV: [%s]", tableName)
		return LTSV
	case "JSON":
		debug.Printf("Guess file type as JSON: [%s]", tableName)
		return JSON
	case "TBLN":
		debug.Printf("Guess file type as TBLN: [%s]", tableName)
		return TBLN
	default:
		debug.Printf("Set in CSV because the extension is unknown: [%s]", tableName)
		return CSV
	}
}

func importFileOpen(tableName string) (io.ReadCloser, error) {
	r := regexp.MustCompile(`\*|\?|\[`)
	if r.MatchString(tableName) {
		return globFileOpen(tableName)
	}
	return singleFileOpen(tableName)
}

func singleFileOpen(fileName string) (io.ReadCloser, error) {
	if len(fileName) == 0 || fileName == "-" || strings.ToLower(fileName) == "stdin" {
		return os.Stdin, nil
	}
	fileName = trimQuote(fileName)
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	return extFileReader(fileName, file), nil
}

func globFileOpen(globName string) (*io.PipeReader, error) {
	globName = trimQuote(globName)
	fileNames, err := filepath.Glob(globName)
	if err != nil {
		return nil, err
	}
	if len(fileNames) == 0 {
		return nil, fmt.Errorf("no matches found: %s", fileNames)
	}
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer func() {
			err = pipeWriter.Close()
			if err != nil {
				log.Printf("pipe close:%s", err)
			}
		}()
		for _, fileName := range fileNames {
			f, err := os.Open(fileName)
			debug.Printf("Open: [%s]", fileName)
			if err != nil {
				log.Printf("ERROR: %s:%s", fileName, err)
				continue
			}
			r := extFileReader(fileName, f)
			_, err = io.Copy(pipeWriter, r)
			if err != nil {
				log.Printf("ERROR: %s:%s", fileName, err)
				continue
			}
			_, err = pipeWriter.Write([]byte("\n"))
			if err != nil {
				log.Printf("ERROR: %s:%s", fileName, err)
				continue
			}
			err = f.Close()
			if err != nil {
				log.Printf("ERROR: %s:%s", fileName, err)
			}
			debug.Printf("Close: [%s]", fileName)
		}
	}()
	return pipeReader, nil
}

func trimQuote(fileName string) string {
	if fileName[0] == '`' {
		fileName = strings.Replace(fileName, "`", "", 2)
	}
	if fileName[0] == '"' {
		fileName = strings.Replace(fileName, "\"", "", 2)
	}
	return fileName
}

func extFileReader(fileName string, reader *os.File) io.ReadCloser {
	if strings.HasSuffix(fileName, ".gz") {
		z, err := gzip.NewReader(reader)
		if err != nil {
			debug.Printf("No gzip file: [%s]", fileName)
			_, err := reader.Seek(0, io.SeekStart)
			if err != nil {
				return nil
			}
			return reader
		}
		debug.Printf("decompress gzip file: [%s]", fileName)
		return z
	}
	return reader
}
