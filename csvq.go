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

func rowimport(db *sql.DB, table string, columns string, row []string) {
	sql := "INSERT INTO " + table + " (" + columns + ") VALUES ('" + strings.Join(row, "','") + "');"
	_, err := db.Exec(sql)
	if err != nil {
		fmt.Println(err)
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
	tablename := "dummy"
	if table != nil {
		tablename = sqlparser.String(table.From)
	}
	return tablename
}

func prerewrite(sqlstr string, oldname string, newname string) (rewrite string) {
	rewrite = strings.Replace(sqlstr, oldname, newname, -1)
	return rewrite
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
	return sqlparser.String(tree)
}

func dbconnect() *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		fmt.Println("ERROR:", err)
	}
	return db
}

func dbdisconnect(db *sql.DB) {
	err := db.Close()
	if err != nil {
		fmt.Println("ERROR:", err)
	}
}

func dbcreate(db *sql.DB, table string, header []string) {
	columns := make([]string, len(header))
	for i := 0; i < len(header); i++ {
		columns[i] = "c" + strconv.Itoa(i+1) + " text"
	}
	c := strings.Join(columns, ",")
	fmt.Println("CREATE TABLE", table, "(", c, ")")
	_, err := db.Exec("CREATE TABLE " + table + "(" + c + ")")
	if err != nil {
		fmt.Println("ERROR:", err)
	}
}

func dbselect(db *sql.DB, sqlstr string) {
	fmt.Println(sqlstr)
	rows, err := db.Query(sqlstr)
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		fmt.Println("ERROR:", err)
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
	sqlstr := os.Args[1]
	fmt.Println("sql:", sqlstr)
	tree, _ := sqlparser.Parse(sqlstr)
	tablename := getTable(tree)
	rtable := strings.Replace(tablename, ".", "_", -1)
	fmt.Println("rewrite:", tablename, rtable)
	sqlr := prerewrite(sqlstr, tablename, rtable)
	fmt.Println("sql:", sqlr)

	header, reader := csvRead(tablename)

	db := dbconnect()
	dbcreate(db, rtable, header)
	csvimport(db, reader, rtable, header)
	dbselect(db, sqlr)
	dbdisconnect(db)
}
