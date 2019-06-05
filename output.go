package trdsql

import (
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode/utf8"
)

type OutputFormat int

// OutPutFormat
const (
	OUT_CSV OutputFormat = iota
	OUT_LTSV
	OUT_JSON
	OUT_RAW
	OUT_MD
	OUT_AT
	OUT_VF
	OUT_TBLN
)

// Writer is file format writer
type Writer interface {
	First([]string, []string) error
	WriteRow([]interface{}, []string) error
	Last() error
}

func (trdsql *TRDSQL) NewWriter() Writer {
	switch trdsql.OutFormat {
	case OUT_LTSV:
		return trdsql.NewLTSVWrite()
	case OUT_JSON:
		return trdsql.NewJSONWrite()
	case OUT_RAW:
		return trdsql.NewRAWWrite()
	case OUT_MD:
		return trdsql.NewTWWrite(true)
	case OUT_AT:
		return trdsql.NewTWWrite(false)
	case OUT_VF:
		return trdsql.NewVFWrite()
	case OUT_TBLN:
		return trdsql.NewTBLNWrite()
	case OUT_CSV:
		return trdsql.NewCSVWrite()
	default:
		return trdsql.NewCSVWrite()
	}
}

type Exporter interface {
	Export(db *DDB, sqlstr string) error
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
		types[i] = convertType(ct.DatabaseTypeName())
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
