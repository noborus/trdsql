package main

import (
	"flag"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"database/sql"
	"encoding/csv"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/xwb1989/sqlparser"
)

var (
	dbdriver string
	dbdsn    string
)

func rowimport(stmt *sql.Stmt, list []interface{}) {
	_, err := stmt.Exec(list...)
	if err != nil {
		log.Println(err)
	}
}

func csvImport(db *sql.DB, reader *csv.Reader, table string, header []string) {
	columns := make([]string, len(header))
	place := make([]string, len(header))
	list := make([]interface{}, len(header))

	for i := range header {
		columns[i] = "c" + strconv.Itoa(i+1)
		if dbdriver == "postgres" {
			place[i] = "$" + strconv.Itoa(i+1)
		} else {
			place[i] = "?"
		}
		list[i] = header[i]
	}
	columnName := strings.Join(columns, ",")
	sqlstr := "INSERT INTO " + table + " (" + columnName + ") VALUES (" + strings.Join(place, ",") + ");"
	stmt, err := db.Prepare(sqlstr)
	if err != nil {
		log.Fatal(err)
	}
	rowimport(stmt, list)

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
		rowimport(stmt, list)
	}
}

func csvOpen(filename string) (*csv.Reader, error) {
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
			// log.Fatal("ERROR: ", err)
			return nil, err
		}
	}
	reader := csv.NewReader(file)
	return reader, err
}

func csvRead(reader *csv.Reader) (header []string) {
	var err error
	header, err = reader.Read()
	if err != nil {
		log.Fatal("ERROR: ", err)
	}
	return header
}

func escapetable(oldname string) (newname string) {
	if dbdriver == "postgres" {
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

func dbConnect(driver, dsn string) *sql.DB {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func dbDisconnect(db *sql.DB) {
	err := db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func dbCreate(db *sql.DB, table string, header []string) {
	columns := make([]string, len(header))
	for i := 0; i < len(header); i++ {
		columns[i] = "c" + strconv.Itoa(i+1) + " text"
	}
	c := strings.Join(columns, ",")
	sqlstr := "CREATE TEMP TABLE " + table + " ( " + c + " )"
	log.Println(sqlstr)
	_, err := db.Exec(sqlstr)
	if err != nil {
		log.Fatal(err)
	}
}

func dbSelect(db *sql.DB, writer *csv.Writer, sqlstr string) {
	sqlstr = strings.TrimSpace(sqlstr)
	if sqlstr == "" {
		log.Fatal("ERROR: no SQL statement")
	}
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

func getSeparator(sepString string) (sepRune rune) {
	sepString = `'` + sepString + `'`
	sepRunes, err := strconv.Unquote(sepString)
	if err != nil {
		log.Fatal(sepString, ": ", err)
	}
	sepRune = ([]rune(sepRunes))[0]

	return sepRune
}

func main() {
	var (
		inSep  string
		outSep string
	)
	flag.StringVar(&dbdriver, "dbdriver", "sqlite3", "database driver.")
	flag.StringVar(&dbdsn, "dbdsn", ":memory:", "database connection option.")
	flag.StringVar(&inSep, "input-delimiter", ",", "Field delimiter for input.")
	flag.StringVar(&inSep, "d", ",", "Field delimiter for input.")
	flag.StringVar(&outSep, "output-delimiter", ",", "Field delimiter for output.")
	flag.StringVar(&outSep, "D", ",", "Field delimiter for output.")
	flag.Parse()
	if len(flag.Args()) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	sqlstr := flag.Args()[0]
	writer := csv.NewWriter(os.Stdout)
	writer.Comma = getSeparator(outSep)
	readerComma := getSeparator(inSep)

	db := dbConnect(dbdriver, dbdsn)
	defer dbDisconnect(db)

	tablenames := sqlparse(sqlstr)
	for _, tablename := range tablenames {
		reader, err := csvOpen(tablename)
		if err != nil {
			continue
		}
		rtable := escapetable(tablename)
		sqlstr = rewrite(sqlstr, tablename, rtable)
		reader.Comma = readerComma
		reader.FieldsPerRecord = -1
		header := csvRead(reader)
		dbCreate(db, rtable, header)
		csvImport(db, reader, rtable, header)
	}
	dbSelect(db, writer, sqlstr)
}
