package main

import (
	"database/sql"
	"encoding/csv"
	_ "fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xwb1989/sqlparser"
)

func rowimport(db *sql.DB, table string, columns string, row []string) {
	place := make([]string, len(row))
	list := make([]interface{}, len(row))
	for i := 0; i < len(row); i++ {
		place[i] = "?"
		list[i] = row[i]
	}
	sqlstr := "INSERT INTO " + table + " (" + columns + ") VALUES (" + strings.Join(place, ",") + ");"
	_, err := db.Exec(sqlstr, list...)
	if err != nil {
		log.Println(row, err)
	}
}

func columnNames(row []string) string {
	columns := make([]string, len(row))
	for i := 0; i < len(row); i++ {
		columns[i] = "c" + strconv.Itoa(i+1)
	}
	return strings.Join(columns, ",")
}

func csvimport(db *sql.DB, reader *csv.Reader, table string, header []string) {
	columns := columnNames(header)
	rowimport(db, table, columns, header)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else {
			if err != nil {
				log.Fatal("ERROR:", err)
			}
		}
		rowimport(db, table, columns, record)
	}
}

func csvRead(filename string) (header []string, reader *csv.Reader) {
	var file *os.File
	var err error
	if filename == "-" {
		file = os.Stdin
	} else {
		if filename[0] == '`' {
			filename = strings.Replace(filename, "`", "", 2)
		}
		file, err = os.Open(filename)
		if err != nil {
			log.Fatal("ERROR:", err)
		}
	}
	reader = csv.NewReader(file)
	header, err = reader.Read()
	if err != nil {
		log.Fatal("ERROR:", err)
	}
	return header, reader
}

func escapetable(oldname string) (newname string) {
	if oldname[0] != '`' {
		newname = "`" + oldname + "`"
	} else {
		newname = oldname
	}
	return newname
}

func rewrite(sqlstr string, oldname string, newname string) (rewrite string) {
	rewrite = strings.Replace(sqlstr, oldname, newname, -1)
	return rewrite
}

func rewriteTable(tree sqlparser.SQLNode, tablename string) string {
	rewriter := func(origin []byte) []byte {
		s := string(origin)
		if s == tablename {
			s = "_"
		}
		return []byte(s)
	}
	sqlparser.Rewrite(tree, rewriter)
	return sqlparser.String(tree)
}

func dbconnect() *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func dbdisconnect(db *sql.DB) {
	err := db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func dbcreate(db *sql.DB, table string, header []string) {
	columns := make([]string, len(header))
	for i := 0; i < len(header); i++ {
		columns[i] = "c" + strconv.Itoa(i+1) + " text"
	}
	c := strings.Join(columns, ",")
	sqlstr := "CREATE TABLE " + table + " ( " + c + " )"
	log.Println(sqlstr)
	_, err := db.Exec(sqlstr)
	if err != nil {
		log.Fatal(err)
	}
}

func dbselect(db *sql.DB, writer *csv.Writer, sqlstr string) {
	log.Println(sqlstr)
	rows, err := db.Query(sqlstr)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
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

func main() {
	sqlstr := os.Args[1]
	writer := csv.NewWriter(os.Stdout)
	writer.Comma = ','

	db := dbconnect()
	defer dbdisconnect(db)

	tablenames := sqlparse(sqlstr)
	for _, tablename := range tablenames {
		rtable := escapetable(tablename)
		sqlstr = rewrite(sqlstr, tablename, rtable)
		header, reader := csvRead(tablename)
		dbcreate(db, rtable, header)
		csvimport(db, reader, rtable, header)
	}
	dbselect(db, writer, sqlstr)
}
