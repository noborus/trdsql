package trdsql

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

// Exporter is the interface for processing query results.
type Exporter interface {
	Export(db *DB, query string) error
}

// WriteOpts is the option to determine the writer process.
type WriteOpts struct {
	OutFormat    Format
	OutDelimiter string
	OutHeader    bool
	OutStream    io.Writer
	ErrStream    io.Writer
}

// NewWriteOpts Returns WriteOpts.
func NewWriteOpts() WriteOpts {
	return WriteOpts{
		OutFormat:    CSV,
		OutDelimiter: ",",
		OutHeader:    false,
		OutStream:    os.Stdout,
		ErrStream:    os.Stderr,
	}
}

// WriteFormat is a structure that includes Writer and WriteOpts,
// and is an implementation of the Exporter interface.
type WriteFormat struct {
	WriteOpts
	Writer
}

// NewExporter returns trdsql default Exporter.
func NewExporter(writeOpts WriteOpts, writer Writer) *WriteFormat {
	return &WriteFormat{
		WriteOpts: writeOpts,
		Writer:    writer,
	}
}

// Export is execute SQL and Exporter the result.
func (e *WriteFormat) Export(db *DB, query string) error {
	rows, err := db.Select(query)
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
		types[i] = ct.DatabaseTypeName()
	}

	err = e.Writer.PreWrite(columns, types)
	if err != nil {
		return err
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}
		err = e.Writer.WriteRow(values, columns)
		if err != nil {
			return err
		}
	}
	return e.Writer.PostWrite()
}

// ValString converts database value to string.
func ValString(v interface{}) string {
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
