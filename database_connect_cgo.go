//go:build cgo

package trdsql

import (
	"database/sql"

	// MySQL driver.
	_ "github.com/go-sql-driver/mysql"

	// PostgreSQL driver.
	_ "github.com/lib/pq"

	// SQLite3 driver.
	_ "github.com/mattn/go-sqlite3"
	// SQlite3 extension library.
	sqlite3_stdlib "github.com/multiprocessio/go-sqlite3-stdlib"
)

var DefaultDriver = "sqlite3"

func init() {
	// Enable sqlite3 extensions.
	// It can be used by setting the driver to "sqlite3_ext".
	sqlite3_stdlib.Register("sqlite3_ext")
}

// Connect is connects to the database.
// Currently supported drivers are sqlite3, mysql, postgres.
// Set quote character and maxBulk depending on the driver type.
func Connect(driver, dsn string) (*DB, error) {
	sqlDB, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}

	db := &DB{
		DB:     sqlDB,
		driver: driver,
		dsn:    dsn,
	}
	debug.Printf("driver: %s, dsn: %s", driver, dsn)

	switch driver {
	case "sqlite3", "sqlite3_ext", "sqlite":
		db.quote = "`"
		db.maxBulk = 10000
	case "mysql":
		db.quote = "`"
		db.maxBulk = 1000
	case "postgres":
		db.quote = "\""
	default:
		db.quote = "\""
	}

	return db, nil
}
