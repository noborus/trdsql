package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func csvOpen(filename string, skip int) (*csv.Reader, error) {
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
