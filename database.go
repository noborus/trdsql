package main

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

// DDB is *sql.DB wrapper.
type DDB struct {
	dbdriver string
	dbdsn    string
	*sql.DB
}

func rowImport(stmt *sql.Stmt, list []interface{}) {
	_, err := stmt.Exec(list...)
	if err != nil {
		log.Println(err)
	}
}

func (db DDB) dbImport(reader *csv.Reader, table string, header []string) {
	columns := make([]string, len(header))
	place := make([]string, len(header))
	list := make([]interface{}, len(header))
	for i := range header {
		columns[i] = "c" + strconv.Itoa(i+1)
		if db.dbdriver == "postgres" {
			place[i] = "$" + strconv.Itoa(i+1)
		} else {
			place[i] = "?"
		}
		list[i] = header[i]
	}
	sqlstr := "INSERT INTO " + table + " (" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(place, ",") + ");"
	stmt, err := db.Prepare(sqlstr)
	if err != nil {
		log.Fatal("ISNERT:", err)
	}
	rowImport(stmt, list)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else {
			if err != nil {
				log.Fatal("ERROR: ", err)
			}
		}
		for i := range header {
			list[i] = record[i]
		}
		rowImport(stmt, list)
	}
}

func dbConnect(driver, dsn string) DDB {
	var db DDB
	var err error
	db.dbdriver = driver
	db.dbdsn = dsn
	db.DB, err = sql.Open(driver, dsn)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func (db DDB) dbDisconnect() {
	err := db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func (db DDB) dbCreate(table string, header []string) {
	var sqlstr string
	columns := make([]string, len(header))
	for i := 0; i < len(header); i++ {
		columns[i] = "c" + strconv.Itoa(i+1) + " text"
	}
	temp := "TEMPORARY"
	sqlstr = "CREATE " + temp + " TABLE "
	sqlstr = sqlstr + table + " ( " + strings.Join(columns, ",") + " );"
	log.Println(sqlstr)
	_, err := db.Exec(sqlstr)
	if err != nil {
		log.Fatal("CREATE:", err)
	}
}

func (db DDB) dbSelect(writer *csv.Writer, sqlstr string) {
	sqlstr = strings.TrimSpace(sqlstr)
	if sqlstr == "" {
		log.Fatal("ERROR: no SQL statement")
	}
	log.Println(sqlstr)
	rows, err := db.Query(sqlstr)
	if err != nil {
		log.Fatal("Query: ", err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("ROWS: ", err)
	}
	values := make([]sql.RawBytes, len(columns))
	results := make([]string, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			log.Fatal(err)
		}
		for i, col := range values {
			results[i] = string(col)
		}
		writer.Write(results)
		writer.Flush()
	}
}

func escapetable(db DDB, oldname string) (newname string) {
	if db.dbdriver == "postgres" {
		if oldname[0] != '"' {
			newname = "\"" + oldname + "\""
		} else {
			newname = oldname
		}
	} else {
		if oldname[0] != '`' {
			newname = "`" + oldname + "`"
		} else {
			newname = oldname
		}
	}
	return newname
}

func rewrite(sqlstr string, oldname string, newname string) (rewrite string) {
	rewrite = strings.Replace(sqlstr, oldname, newname, -1)
	return rewrite
}

func sqlparse(sqlstr string) []string {
	word := strings.Fields(sqlstr)
	tablenames := make([]string, 0, 1)
	for i := 0; i < len(word); i++ {
		if element := strings.ToUpper(word[i]); element == "FROM" || element == "JOIN" {
			tablenames = append(tablenames, word[i+1])
		}
	}
	return tablenames
}
