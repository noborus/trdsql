package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"encoding/csv"
)

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

func main() {
	var (
		odbdriver string
		odbdsn    string
		inSep     string
		outSep    string
	)
	dbdriver := "sqlite3"
	dbdsn := ""
	cfg, err := loadConfig()
	if err != nil {
		log.Println("no config")
	} else {
		dbdriver = cfg.Dbdriver
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `
Usage: %s [OPTIONS] [SQL]

Options:
`, os.Args[0])
		flag.PrintDefaults()
	}

	flag.StringVar(&odbdriver, "dbdriver", "", "database driver. default sqlite3")
	flag.StringVar(&odbdsn, "dbdsn", "", "database connection option.")
	flag.StringVar(&inSep, "input-delimiter", ",", "Field delimiter for input.")
	flag.StringVar(&inSep, "d", ",", "Field delimiter for input.")
	flag.StringVar(&outSep, "output-delimiter", ",", "Field delimiter for output.")
	flag.StringVar(&outSep, "D", ",", "Field delimiter for output.")
	flag.Parse()
	if odbdriver != "" {
		dbdriver = odbdriver
	}
	if odbdsn != "" {
		dbdsn = odbdsn
	}
	if len(flag.Args()) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	sqlstr := flag.Args()[0]
	if dbdsn == "" {
		for _, c := range cfg.Target {
			if dbdriver == c.Name {
				log.Println(c.Name, c.Dsn)
				dbdsn = c.Dsn
			}
		}
		if dbdriver == "sqlite3" {
			dbdsn = ":memory:"
		}
	}

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
		rtable := escapetable(db, tablename)
		sqlstr = rewrite(sqlstr, tablename, rtable)
		reader.Comma = readerComma
		reader.FieldsPerRecord = -1
		header := csvRead(reader)
		dbCreate(db, rtable, header)
		dbImport(db, reader, rtable, header)
	}
	dbSelect(db, writer, sqlstr)
}
