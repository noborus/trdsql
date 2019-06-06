package trdsql

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"strings"
	"time"
	"unicode/utf8"
)

type WriteOpts struct {
	OutFormat    Format
	OutDelimiter string
	OutHeader    bool
	OutStream    io.Writer
	ErrStream    io.Writer
}

// Writer is file format writer
type Writer interface {
	First([]string, []string) error
	WriteRow([]interface{}, []string) error
	Last() error
}

func NewWriter() Writer {
	switch DefaultWriteOpts.OutFormat {
	case LTSV:
		return NewLTSVWrite()
	case JSON:
		return NewJSONWrite()
	case RAW:
		return NewRAWWrite(DefaultWriteOpts.OutDelimiter, DefaultWriteOpts.OutHeader)
	case MD:
		return NewTWWrite(true)
	case AT:
		return NewTWWrite(false)
	case VF:
		return NewVFWrite()
	case TBLN:
		return NewTBLNWrite()
	case CSV:
		return NewCSVWrite(DefaultWriteOpts.OutDelimiter, DefaultWriteOpts.OutHeader)
	default:
		return NewCSVWrite(DefaultWriteOpts.OutDelimiter, DefaultWriteOpts.OutHeader)
	}
}

// Export is execute SQL and Exporter the result.
func (trdsql *TRDSQL) Export(db *DDB, sqlstr string) error {
	w := trdsql.Writer
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
		types[i] = ct.DatabaseTypeName()
	}

	err = w.First(columns, types)
	if err != nil {
		return err
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}
		err = w.WriteRow(values, columns)
		if err != nil {
			return err
		}
	}
	return w.Last()
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
