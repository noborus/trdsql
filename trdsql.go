package trdsql

import (
	"fmt"
	"io"
	"log"
	"os"
)

// TRDSQL structure is a structure that defines the whole operation.
type TRDSQL struct {
	Driver string
	Dsn    string
	Sql    string

	InFormat    InputFormat
	InPreRead   int
	InSkip      int
	InDelimiter string
	InHeader    bool

	OutFormat    OutputFormat
	OutStream    io.Writer
	ErrStream    io.Writer
	OutDelimiter string
	OutHeader    bool
}

func NewTRDSQL() *TRDSQL {
	return &TRDSQL{
		Driver:    "sqlite3",
		Dsn:       "",
		Sql:       "",
		OutStream: os.Stdout,
		ErrStream: os.Stderr,
	}
}

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

	db.tx, err = db.Begin()
	if err != nil {
		return fmt.Errorf("ERROR(BEGIN):%s", err)
	}

	trdsql.Sql, err = trdsql.Import(db, trdsql.Sql)
	if err != nil {
		return fmt.Errorf("ERROR(IMPORT):%s", err)
	}

	err = trdsql.Export(db, trdsql.Sql)
	if err != nil {
		return fmt.Errorf("ERROR(EXPORT):%s", err)
	}

	err = db.tx.Commit()
	if err != nil {
		return fmt.Errorf("ERROR(COMMIT):%s", err)
	}

	return nil
}

type DebugT bool

var debug = DebugT(false)

func DebugEnable() {
	debug = true
}

func (d DebugT) Printf(format string, args ...interface{}) {
	if d {
		log.Printf(format, args...)
	}
}
