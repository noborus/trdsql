// package trdsql execute SQL queries on csv and other tabular data.
package trdsql

import (
	"fmt"
	"log"
)

// TRDSQL structure is a structure that defines the whole operation.
type TRDSQL struct {
	Driver   string
	Dsn      string
	Importer Importer
	Exporter Exporter
}

func NewTRDSQL(im Importer, ex Exporter) *TRDSQL {
	return &TRDSQL{
		Driver:   "sqlite3",
		Dsn:      "",
		Importer: im,
		Exporter: ex,
	}
}

// Format represents the input/output format
type Format int

// Represents Format
const (
	// READ (guesses for read format)
	GUESS Format = iota
	// READ/WRITE
	CSV
	// READ/WRITE
	LTSV
	// READ/WRITE
	JSON
	// READ/WRITE
	TBLN
	// WRITE
	RAW
	// WRITE
	MD
	// WRITE
	AT
	// WRITE
	VF
)

func (f Format) String() string {
	switch f {
	case GUESS:
		return "GUESS"
	case CSV:
		return "CSV"
	case LTSV:
		return "LTSV"
	case JSON:
		return "JSON"
	case TBLN:
		return "TBLN"
	case RAW:
		return "RAW"
	case MD:
		return "MD"
	case AT:
		return "AT"
	case VF:
		return "VF"
	default:
		return "Unknown"
	}
}

// Default database type
const DefaultDBType = "text"

func (trd *TRDSQL) Exec(sql string) error {
	db, err := Connect(trd.Driver, trd.Dsn)
	if err != nil {
		return fmt.Errorf("ERROR(CONNECT):%s", err)
	}
	defer func() {
		err = db.Disconnect()
		if err != nil {
			log.Printf("ERROR(DISCONNECT):%s", err)
		}
	}()

	db.Tx, err = db.Begin()
	if err != nil {
		return fmt.Errorf("ERROR(BEGIN):%s", err)
	}

	if trd.Importer != nil {
		sql, err = trd.Importer.Import(db, sql)
		if err != nil {
			return fmt.Errorf("ERROR(IMPORT):%s", err)
		}
	}

	if trd.Exporter != nil {
		err = trd.Exporter.Export(db, sql)
		if err != nil {
			return fmt.Errorf("ERROR(EXPORT):%s", err)
		}
	}

	err = db.Tx.Commit()
	if err != nil {
		return fmt.Errorf("ERROR(COMMIT):%s", err)
	}

	return nil
}
