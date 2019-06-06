package trdsql

import (
	"fmt"
	"log"
	"os"
)

// TRDSQL structure is a structure that defines the whole operation.
type TRDSQL struct {
	Driver string
	Dsn    string

	SQL string

	InFormat    Format
	InPreRead   int
	InSkip      int
	InDelimiter string
	InHeader    bool

	Writer Writer
}

func NewTRDSQL() *TRDSQL {
	return &TRDSQL{
		Driver:      "sqlite3",
		Dsn:         "",
		SQL:         "",
		InDelimiter: ",",
		InPreRead:   1,
	}
}

var DefaultWriteOpts = &WriteOpts{
	OutDelimiter: ",",
	OutHeader:    false,
	OutStream:    os.Stdout,
	ErrStream:    os.Stderr,
}

// Format represents the input/output format
type Format int

// Represents Format
const (
	GUESS Format = iota
	CSV
	LTSV
	JSON
	TBLN
	RAW
	MD
	AT
	VF
)

func (trdsql *TRDSQL) Exec() error {
	db, err := Connect(trdsql.Driver, trdsql.Dsn)
	if err != nil {
		return fmt.Errorf("ERROR(CONNECT):%s", err)
	}
	defer func() {
		err = db.Disconnect()
		if err != nil {
			log.Printf("ERROR(DISCONNECT):%s", err)
		}
	}()

	if trdsql.Writer == nil {
		trdsql.Writer = NewWriter()
	}

	db.tx, err = db.Begin()
	if err != nil {
		return fmt.Errorf("ERROR(BEGIN):%s", err)
	}

	trdsql.SQL, err = trdsql.Import(db, trdsql.SQL)
	if err != nil {
		return fmt.Errorf("ERROR(IMPORT):%s", err)
	}

	err = trdsql.Export(db, trdsql.SQL)
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
