package trdsql

import (
	"fmt"
	"io"
	"log"
	"os"
)

// TRDSQL structure is a structure that defines the whole operation.
type TRDSQL struct {
	Driver    string
	Dsn       string
	SQL       string
	ReadOpts  ReadOpts
	WriteOpts WriteOpts
	Writer    Writer
}

func NewTRDSQL() *TRDSQL {
	return &TRDSQL{
		Driver:    "sqlite3",
		Dsn:       "",
		SQL:       "",
		ReadOpts:  NewReadOpts(),
		WriteOpts: NewWriteOpts(),
	}
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

func (trd *TRDSQL) Exec() error {
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

	if trd.Writer == nil {
		trd.Writer = trd.NewWriter()
	}

	db.tx, err = db.Begin()
	if err != nil {
		return fmt.Errorf("ERROR(BEGIN):%s", err)
	}

	trd.SQL, err = trd.Import(db, trd.SQL)
	if err != nil {
		return fmt.Errorf("ERROR(IMPORT):%s", err)
	}

	err = trd.Export(db, trd.SQL)
	if err != nil {
		return fmt.Errorf("ERROR(EXPORT):%s", err)
	}

	err = db.tx.Commit()
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
