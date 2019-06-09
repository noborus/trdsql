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

type Exporter interface {
	Export(db *DB, query string) error
}

type WriteOpts struct {
	OutFormat    Format
	OutDelimiter string
	OutHeader    bool
	OutStream    io.Writer
	ErrStream    io.Writer
}

func NewWriteOpts() WriteOpts {
	return WriteOpts{
		OutDelimiter: ",",
		OutHeader:    false,
		OutStream:    os.Stdout,
		ErrStream:    os.Stderr,
	}
}

type exporter struct {
	WriteOpts
	Writer
}

func NewExporter(writeOpts WriteOpts, writer Writer) *exporter {
	return &exporter{
		WriteOpts: writeOpts,
		Writer:    writer,
	}
}

// Export is execute SQL and Exporter the result.
func (e *exporter) Export(db *DB, query string) error {
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

	err = e.Writer.First(columns, types)
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
	return e.Writer.Last()
}

func ConvertTypes(dbTypes []string) []string {
	ret := make([]string, len(dbTypes))
	for i, t := range dbTypes {
		ret[i] = convertType(t)
	}
	return ret
}

func convertType(dbType string) string {
	switch strings.ToLower(dbType) {
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

// Writer is file format writer
type Writer interface {
	First([]string, []string) error
	WriteRow([]interface{}, []string) error
	Last() error
}

func NewWriter(writeOpts WriteOpts) Writer {
	switch writeOpts.OutFormat {
	case LTSV:
		return NewLTSVWrite(writeOpts)
	case JSON:
		return NewJSONWrite(writeOpts)
	case RAW:
		return NewRAWWrite(writeOpts)
	case MD:
		return NewTWWrite(writeOpts, true)
	case AT:
		return NewTWWrite(writeOpts, false)
	case VF:
		return NewVFWrite(writeOpts)
	case TBLN:
		return NewTBLNWrite(writeOpts)
	case CSV:
		return NewCSVWrite(writeOpts)
	default:
		return NewCSVWrite(writeOpts)
	}
}
