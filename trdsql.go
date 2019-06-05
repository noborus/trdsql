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

	SQL string

	InFormat    InputFormat
	InPreRead   int
	InSkip      int
	InDelimiter string
	InHeader    bool

	Writer Writer

	OutFormat    OutputFormat
	OutStream    io.Writer
	ErrStream    io.Writer
	OutDelimiter string
	OutHeader    bool
}

func NewTRDSQL() *TRDSQL {
	return &TRDSQL{
		Driver:       "sqlite3",
		Dsn:          "",
		SQL:          "",
		InDelimiter:  ",",
		InPreRead:    1,
		OutDelimiter: ",",
		OutStream:    os.Stdout,
		ErrStream:    os.Stderr,
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

	if trdsql.Writer == nil {
		trdsql.Writer = trdsql.NewWriter()
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
