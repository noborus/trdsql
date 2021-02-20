package trdsql

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

var (
	// ErrInvalidColumn is returned if invalid column.
	ErrInvalidColumn = errors.New("invalid column")
	// ErrNoReader is returned when there is no reader.
	ErrNoReader = errors.New("no reader")
	// ErrUnknownFormat is returned if the format is unknown.
	ErrUnknownFormat = errors.New("unknown format")
	// ErrNoRows returned when there are no rows.
	ErrNoRows = errors.New("no rows")
	// ErrUnableConvert is returned if it cannot be converted to a table.
	ErrUnableConvert = errors.New("unable to convert")
	// ErrNoMatchFound is returned if no match is found.
	ErrNoMatchFound = errors.New("no match found")
	// ErrNonDefinition is returned when there is no definition.
	ErrNonDefinition = errors.New("no definition")
)

// Importer is the interface import data into the database.
// Importer parses sql query to decide which file to Import.
// Therefore, the reader does not receive it directly.
type Importer interface {
	Import(db *DB, query string) (string, error)
	ImportContext(ctx context.Context, db *DB, query string) (string, error)
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
	ctx := context.Background()
	return i.ImportContext(ctx, db, query)
}

// ImportContext is parses the SQL statement and imports one or more tables.
// ImportContext is called from ExecContext.
// Return the rewritten SQL and error.
// No error is returned if there is no table to import.
func (i *ReadFormat) ImportContext(ctx context.Context, db *DB, query string) (string, error) {
	parsedQuery := SQLFields(query)
	tables, tableIdx := TableNames(parsedQuery)
	if len(tables) == 0 {
		// without FROM clause. ex. SELECT 1+1;
		debug.Printf("table not found\n")
		return query, nil
	}

	for fileName := range tables {
		tableName, err := ImportFileContext(ctx, db, fileName, i.ReadOpts)
		if err != nil {
			return query, err
		}
		if len(tableName) > 0 {
			tables[fileName] = tableName
		}
	}

	// replace table names in query with their quoted values
	for _, idx := range tableIdx {
		if table, ok := tables[parsedQuery[idx]]; ok {
			parsedQuery[idx] = table
		}
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
		case strings.EqualFold(w, "FROM"),
			strings.EqualFold(w, "JOIN"),
			strings.EqualFold(w, "TABLE"):
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
		case ' ', '\t', '\r', '\n', ',', ';', '=', '(', ')':
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
	return ImportFileContext(context.Background(), db, fileName, readOpts)
}

// ImportFileContext is imports a file.
// Return the quoted table name and error.
// Do not import if file not found (no error).
// Wildcards can be passed as fileName.
func ImportFileContext(ctx context.Context, db *DB, fileName string, readOpts *ReadOpts) (string, error) {
	file, err := importFileOpen(fileName)
	if err != nil {
		if !strings.Contains(fileName, "::") {
			debug.Printf("%s\n", err)
			return "", nil
		}
		// path notation.
		readOpts.InPath = fileName[strings.Index(fileName, "::")+2:]
		fileName = fileName[0:strings.Index(fileName, "::")]
		file, err = importFileOpen(fileName)
		if err != nil {
			debug.Printf("%s\n", err)
			return "", nil
		}
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("file close:%s", err)
		}
	}()

	readOpts = realFormat(fileName, readOpts)
	reader, err := NewReader(file, readOpts)
	if err != nil {
		return "", err
	}

	tableName := db.QuotedName(fileName)
	if readOpts.InPath != "" {
		tableName = db.QuotedName(fileName + "::" + readOpts.InPath)
	}

	columnNames, err := reader.Names()
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return tableName, err
		}
		debug.Printf("EOF reached before argument number of rows")
	}
	columnTypes, err := reader.Types()
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return tableName, err
		}
		debug.Printf("EOF reached before argument number of rows")
	}
	debug.Printf("Column Names: [%v]", strings.Join(columnNames, ","))
	debug.Printf("Column Types: [%v]", strings.Join(columnTypes, ","))

	if err := db.CreateTableContext(ctx, tableName, columnNames, columnTypes, readOpts.IsTemporary); err != nil {
		return tableName, err
	}

	return tableName, db.ImportContext(ctx, tableName, columnNames, reader)
}

func realFormat(fileName string, readOpts *ReadOpts) *ReadOpts {
	if readOpts.InFormat != GUESS {
		readOpts.realFormat = readOpts.InFormat
		return readOpts
	}

	readOpts.realFormat = guessFormat(fileName)
	if readOpts.realFormat == JSON && readOpts.InPath != "" {
		readOpts.realFormat = JSONPATH
	}
	debug.Printf("Guess file type as %s: [%s]", readOpts.realFormat, fileName)
	return readOpts
}

// guessFormat is guess format from the file name extension.
// Format extensions are searched recursively to remove
// compression extensions such as .gz.
func guessFormat(tableName string) Format {
	tableName = strings.TrimRight(tableName, "\"'`")
	for {
		dotExt := filepath.Ext(tableName)
		if dotExt == "" {
			debug.Printf("Set in CSV because the extension is unknown: [%s]", tableName)
			return CSV
		}
		ext := strings.ToUpper(strings.TrimLeft(dotExt, "."))
		switch ext {
		case "CSV":
			return CSV
		case "LTSV":
			return LTSV
		case "JSON", "JSONL":
			return JSON
		case "TBLN":
			return TBLN
		}
		tableName = tableName[0 : len(tableName)-len(dotExt)]
	}
}

func importFileOpen(tableName string) (io.ReadCloser, error) {
	r := regexp.MustCompile(`\*|\?|\[`)
	if r.MatchString(tableName) {
		return globFileOpen(tableName)
	}
	return singleFileOpen(tableName)
}

func uncompressedReader(reader io.Reader) io.ReadCloser {
	var err error
	buf := [7]byte{}
	n, err := io.ReadAtLeast(reader, buf[:], len(buf))
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return ioutil.NopCloser(bytes.NewReader(buf[:n]))
		}
		return ioutil.NopCloser(bytes.NewReader(nil))
	}

	rd := io.MultiReader(bytes.NewReader(buf[:n]), reader)
	var r io.ReadCloser
	switch {
	case bytes.Equal(buf[:3], []byte{0x1f, 0x8b, 0x8}):
		r, err = gzip.NewReader(rd)
	case bytes.Equal(buf[:3], []byte{0x42, 0x5A, 0x68}):
		r = ioutil.NopCloser(bzip2.NewReader(rd))
	case bytes.Equal(buf[:4], []byte{0x28, 0xb5, 0x2f, 0xfd}):
		var zr *zstd.Decoder
		zr, err = zstd.NewReader(rd)
		r = ioutil.NopCloser(zr)
	case bytes.Equal(buf[:4], []byte{0x04, 0x22, 0x4d, 0x18}):
		r = ioutil.NopCloser(lz4.NewReader(rd))
	case bytes.Equal(buf[:7], []byte{0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x0, 0x0}):
		var zr *xz.Reader
		zr, err = xz.NewReader(rd)
		r = ioutil.NopCloser(zr)
	}

	if err != nil || r == nil {
		r = ioutil.NopCloser(rd)
	}
	return r
}

func singleFileOpen(fileName string) (io.ReadCloser, error) {
	if len(fileName) == 0 || fileName == "-" || strings.ToLower(fileName) == "stdin" {
		return uncompressedReader(bufio.NewReader(os.Stdin)), nil
	}
	fileName = expandTilde(trimQuote(fileName))
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	return uncompressedReader(file), nil
}

func globFileOpen(globName string) (*io.PipeReader, error) {
	globName = expandTilde(trimQuote(globName))
	fileNames, err := filepath.Glob(globName)
	if err != nil {
		return nil, err
	}
	if len(fileNames) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoMatchFound, fileNames)
	}
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer func() {
			if err := pipeWriter.Close(); err != nil {
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
			r := uncompressedReader(f)

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

			if err := f.Close(); err != nil {
				log.Printf("ERROR: %s:%s", fileName, err)
			}
			debug.Printf("Close: [%s]", fileName)
		}
	}()
	return pipeReader, nil
}

func expandTilde(fileName string) string {
	if strings.HasPrefix(fileName, "~") {
		usr, err := user.Current()
		if err != nil {
			log.Printf("ERROR: %s", err)
			return fileName
		}
		fileName = filepath.Join(usr.HomeDir, fileName[1:])
	}
	return fileName
}

func trimQuote(fileName string) string {
	if fileName[0] == '`' && fileName[len(fileName)-1] == '`' {
		fileName = fileName[1 : len(fileName)-1]
	}
	if fileName[0] == '"' && fileName[len(fileName)-1] == '"' {
		fileName = fileName[1 : len(fileName)-1]
	}
	return fileName
}
