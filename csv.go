package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

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

func getSeparator(sepString string) (sepRune rune) {
	sepRune = ','
	sepString = `'` + sepString + `'`
	sepRunes, err := strconv.Unquote(sepString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getSeparator: %s", err)
	} else {
		sepRune = ([]rune(sepRunes))[0]
	}
	return sepRune
}
