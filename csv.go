package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func csvOpen(filename string, delimiter string, skip int) (*csv.Reader, error) {
	var file *os.File
	var err error
	if filename == "-" {
		file = os.Stdin
	} else {
		if filename[0] == '`' {
			filename = strings.Replace(filename, "`", "", 2)
		}
		if filename[0] == '"' {
			filename = strings.Replace(filename, "\"", "", 2)
		}
		file, err = os.Open(filename)
		if err != nil {
			return nil, err
		}
	}
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // no check count
	reader.TrimLeadingSpace = true
	reader.Comma, err = getSeparator(delimiter)
	if err != nil {
		return nil, err
	}
	for i := 0; i < skip; i++ {
		r, _ := reader.Read()
		debug.Printf("Skip row:%s\n", strings.Join(r, " "))
	}
	return reader, err
}

func headerRead(reader *csv.Reader) ([]string, error) {
	var err error
	var header []string
	header, err = reader.Read()
	return header, err
}

func getSeparator(sepString string) (rune, error) {
	sepRunes, err := strconv.Unquote(`'` + sepString + `'`)
	if err != nil {
		return ',', fmt.Errorf("ERROR getSeparator: %s:%s", err, sepString)
	}
	sepRune := ([]rune(sepRunes))[0]
	return sepRune, err
}

func (trdsql TRDSQL) csvReader(db *DDB, sqlstr string, tablenames []string) (string, int) {
	var header []string
	for _, tablename := range tablenames {
		reader, err := csvOpen(tablename, trdsql.inSep, trdsql.iskip)
		if err != nil {
			// no file
			continue
		}
		rtable := db.escapetable(tablename)
		sqlstr = rewrite(sqlstr, tablename, rtable)
		header, err = headerRead(reader)
		if err != nil {
			log.Println(err)
			return sqlstr, 1
		}
		db.Create(rtable, header, trdsql.ihead)
		err = db.ImportPrepare(rtable, header, trdsql.ihead)
		if err != nil {
			log.Println(err)
			return sqlstr, 1
		}
		db.Import(reader, header, trdsql.ihead)
	}
	return sqlstr, 0
}

func (trdsql TRDSQL) csvWrite(db *DDB, sqlstr string) int {
	var err error
	writer := csv.NewWriter(trdsql.outStream)
	writer.Comma, err = getSeparator(trdsql.outSep)
	if err != nil {
		log.Println(err)
	}
	err = db.Output(writer, sqlstr, trdsql.outHeader)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}
