package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xwb1989/sqlparser"
)

func csvprint(db *sql.DB, table string, row []string) {
	columns := make([]string, len(row))
	for i := 0; i < len(row); i++ {
		columns[i] = "c" + strconv.Itoa(i+1)
	}
	c := strings.Join(columns, ",")
	sql := "INSERT INTO " + table + " (" + c + ") VALUES ('" + strings.Join(row, "','") + "');"
	//	fmt.Println(sql)
	_, err := db.Exec(sql)
	if err != nil {
		fmt.Println(err)
	}
}

func rowRead(db *sql.DB, reader *csv.Reader, table string, header []string) {
	if header != nil {
		csvprint(db, table, header)
	}
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else {
			if err != nil {
				log.Fatal("ERROR:", err)
			}
		}
		csvprint(db, table, record)
	}
}

func csvRead(db *sql.DB, filename string) (header []string, reader *csv.Reader) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("ERROR:", err)
	}
	reader = csv.NewReader(file)
	header, err = reader.Read()
	if err != nil {
		log.Fatal("ERROR:", err)
	}
	return header, reader
}

func getTable(sqlNode sqlparser.SQLNode) string {
	table, _ := sqlNode.(*sqlparser.Select)
	tablename := sqlparser.String(table.From)
	fmt.Println(tablename)
	return tablename
}

func rewriteTable(tree sqlparser.SQLNode, tablename string) string {
	rewriter := func(origin []byte) []byte {
		fmt.Println("element:", string(origin))
		s := string(origin)
		if s == tablename {
			s = "_"
		}
		return []byte(s)
	}
	sqlparser.Rewrite(tree, rewriter)
	/*
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("%v", tree)
		pq := buf.ParsedQuery()
		bytes, _ := pq.GenerateQuery(map[string]interface{}{"id": 1})
		return string(bytes)
	*/
	return sqlparser.String(tree)
}

func dbconnect() *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		fmt.Println(err)
	}
	return db
}

func dbcreate(db *sql.DB, table string, header []string) *sql.DB {
	columns := make([]string, len(header))
	for i := 0; i < len(header); i++ {
		columns[i] = "c" + strconv.Itoa(i+1) + " text"
	}
	c := strings.Join(columns, ",")
	fmt.Println(c)
	_, err := db.Exec("CREATE TABLE " + table + "(" + c + ")")
	if err != nil {
		fmt.Println(err)
	}
	return db
}

func dbselect(db *sql.DB, sqlstr string) {
	rows, err := db.Query(sqlstr)
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("rows length:%d\n", len(columns))
	values := make([]sql.RawBytes, len(columns))
	results := make([]string, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			fmt.Println(err)
		}
		for i, col := range values {
			results[i] = string(col)
		}
		result := strings.Join(results, ",")
		fmt.Println(result)
	}
}

func main() {
	sqls := os.Args[1]
	fmt.Println("orgin:", sqls)
	tree, _ := sqlparser.Parse(sqls)
	table := getTable(tree)
	fmt.Println("tablename:", table)
	rewrite := rewriteTable(tree, table)
	fmt.Println("rewrite:", rewrite)
	db := dbconnect()
	header, reader := csvRead(db, table)
	db = dbcreate(db, "_", header)
	rowRead(db, reader, "_", header)
	dbselect(db, rewrite)
}
