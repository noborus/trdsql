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
	parsedQuery := SQLFields(query)
	tables, tableIdx := TableNames(parsedQuery)
	if len(tables) == 0 {
		// without FROM clause. ex. SELECT 1+1;
		debug.Printf("table not found\n")
		return query, nil
	}
	for fileName := range tables {
		tableName, err := ImportFile(db, fileName, i.ReadOpts)
		if err != nil {
			return query, err
		}
		if len(tableName) > 0 {
			tables[fileName] = tableName
		}
	}

	// replace table names in query with their quoted values
	for _, idx := range tableIdx {
		parsedQuery[idx] = tables[parsedQuery[idx]]
	}

	// reconstruct the query with quoted table names
	query = strings.Join(parsedQuery, "")
	return query, nil
}

// TableNames returns a map of table names
// that may be tables by a simple SQL parser
// from the query string of the argument,
// along with the locations within the parsed
// query where those table names were found.
func TableNames(parsedQuery []string) (map[string]string, []int) {
	tables := make(map[string]string)
	tableIdx := []int{}
	tableFlag := false
	frontFlag := false
	debug.Printf("[%s]", strings.Join(parsedQuery, "]["))
	for i, w := range parsedQuery {
		switch {
		case strings.Contains(" \t\r\n;=", w):
			continue
		case strings.ToUpper(w) == "FROM" || strings.ToUpper(w) == "JOIN":
			tableFlag = true
			frontFlag = true
		case isSQLKeyWords(w):
			tableFlag = false
		case w == ",":
			frontFlag = true
		default:
			if tableFlag && frontFlag {
				if w[len(w)-1] == ')' {
					w = w[:len(w)-1]
				}
				if !isSQLKeyWords(w) {
					tables[w] = w
					tableIdx = append(tableIdx, i)
				}
			}
			frontFlag = false
		}
	}
	return tables, tableIdx
}

// SQLFields returns an array of string fields
// (interpreting quotes) from the argument query.
func SQLFields(query string) []string {
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
				parsed = append(parsed, string(r))
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
	if len(buf) > 0 {
		parsed = append(parsed, buf)
	}
	return parsed
}

func isSQLKeyWords(str string) bool {
	switch strings.ToUpper(str) {
	case "WHERE", "GROUP", "HAVING", "WINDOW", "UNION", "ORDER", "LIMIT", "OFFSET", "FETCH",
		"FOR", "LEFT", "RIGHT", "CROSS", "INNER", "FULL", "LATERAL", "(SELECT":
		return true
	}
	return false
}

// ImportFile is imports a file.
// Return the quoted table name and error.
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

	tableName := db.QuotedName(fileName)
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
	ext = strings.TrimRight(ext, "\"'`")
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
