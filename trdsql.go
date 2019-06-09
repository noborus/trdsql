package trdsql

import (
	"fmt"
	"io"
	"log"
	"os"
)

// TRDSQL structure is a structure that defines the whole operation.
type TRDSQL struct {
	Driver   string
	Dsn      string
	Importer Importer
	Exporter Exporter
}

type ReadOpts struct {
	InFormat    Format
	InPreRead   int
	InSkip      int
	InDelimiter string
	InHeader    bool
}

type WriteOpts struct {
	OutFormat    Format
	OutDelimiter string
	OutHeader    bool
	OutStream    io.Writer
	ErrStream    io.Writer
}

func NewReadOpts() ReadOpts {
	return ReadOpts{
		InDelimiter: ",",
		InHeader:    false,
		InPreRead:   1,
		InSkip:      0,
	}
}

func NewWriteOpts() WriteOpts {
	return WriteOpts{
		OutDelimiter: ",",
		OutHeader:    false,
		OutStream:    os.Stdout,
		ErrStream:    os.Stderr,
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

// Default database type
const DefaultDBType = "text"

func NewTRDSQL(im Importer, ex Exporter) *TRDSQL {
	return &TRDSQL{
		Driver:   "sqlite3",
		Dsn:      "",
		Importer: im,
		Exporter: ex,
	}
}

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

type debugT bool

var debug = debugT(false)

func DebugEnable() {
	debug = true
}

func (d debugT) Printf(format string, args ...interface{}) {
	if d {
		log.Printf(format, args...)
	}
}
