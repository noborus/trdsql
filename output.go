package trdsql

import (
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode/utf8"
)

// Writer is file format writer
type Writer interface {
	First([]string, []string) error
	WriteRow([]interface{}, []string) error
	Last() error
}

func (trd *TRDSQL) NewWriter() Writer {
	switch trd.WriteOpts.OutFormat {
	case LTSV:
		return NewLTSVWrite(trd.WriteOpts)
	case JSON:
		return NewJSONWrite(trd.WriteOpts)
	case RAW:
		return NewRAWWrite(trd.WriteOpts)
	case MD:
		return NewTWWrite(trd.WriteOpts, true)
	case AT:
		return NewTWWrite(trd.WriteOpts, false)
	case VF:
		return NewVFWrite(trd.WriteOpts)
	case TBLN:
		return NewTBLNWrite(trd.WriteOpts)
	case CSV:
		return NewCSVWrite(trd.WriteOpts)
	default:
		return NewCSVWrite(trd.WriteOpts)
	}
}

type ExportFunc func(db *DDB, sqlstr string, writer Writer)

var Export ExportFunc

type Exporter interface {
	Export(db *DDB, sqlstr string, writer Writer) error
}

// Export is execute SQL and Exporter the result.
func (f *ExportFunc) Export(db *DDB, sqlstr string, writer Writer) error {
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

	err = writer.First(columns, types)
	if err != nil {
		return err
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}
		err = writer.WriteRow(values, columns)
		if err != nil {
			return err
		}
	}
	return writer.Last()
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
