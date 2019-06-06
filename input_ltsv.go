package trdsql

import (
	"bufio"
	"errors"
	"io"
	"log"
	"strings"
)

// LTSVRead provides methods of the Reader interface
type LTSVRead struct {
	reader    *bufio.Reader
	preRead   []map[string]string
	delimiter string
	names     []string
	types     []string
}

func NewLTSVReader(r io.Reader, opts ReadOpts) (Reader, error) {
	lr := &LTSVRead{}
	lr.reader = bufio.NewReader(r)
	lr.delimiter = "\t"

	if opts.InSkip > 0 {
		skip := make([]interface{}, 1)
		for i := 0; i < opts.InSkip; i++ {
			r, e := lr.ReadRow(skip)
			if e != nil {
				log.Printf("ERROR: skip error %s", e)
				break
			}
			debug.Printf("Skip row:%s\n", r)
		}
	}

	return lr, nil
}

// GetColumn is reads the specified number of rows and determines the column name.
// The previously read row is stored in preRead.
func (lr *LTSVRead) GetColumn(rowNum int) ([]string, error) {
	names := map[string]bool{}
	for i := 0; i < rowNum; i++ {
		row, keys, err := lr.read()
		if err != nil {
			return lr.names, err
		}
		// Add only unique column names.
		for k := 0; k < len(keys); k++ {
			if !names[keys[k]] {
				names[keys[k]] = true
				lr.names = append(lr.names, keys[k])
			}
		}
		lr.preRead = append(lr.preRead, row)
	}
	return lr.names, nil
}

// GetTypes is reads the specified number of rows and determines the column type.
func (lr *LTSVRead) GetTypes() ([]string, error) {
	lr.types = make([]string, len(lr.names))
	for i := 0; i < len(lr.names); i++ {
		lr.types[i] = DefaultDBType
	}
	return lr.types, nil
}

// PreReadRow is returns only columns that store preread rows.
func (lr *LTSVRead) PreReadRow() [][]interface{} {
	rowNum := len(lr.preRead)
	rows := make([][]interface{}, rowNum)
	for n := 0; n < rowNum; n++ {
		rows[n] = make([]interface{}, len(lr.names))
		for i := range lr.names {
			rows[n][i] = lr.preRead[n][lr.names[i]]
		}
	}
	return rows
}

// ReadRow is read the rest of the row.
func (lr *LTSVRead) ReadRow(row []interface{}) ([]interface{}, error) {
	record, _, err := lr.read()
	if err != nil {
		return row, err
	}
	for i := range lr.names {
		row[i] = record[lr.names[i]]
	}
	return row, nil
}

func (lr *LTSVRead) read() (map[string]string, []string, error) {
	line, err := lr.readline()
	if err != nil {
		return nil, nil, err
	}
	columns := strings.Split(line, lr.delimiter)
	lvs := make(map[string]string)
	keys := make([]string, 0, len(columns))
	for _, column := range columns {
		kv := strings.SplitN(column, ":", 2)
		if len(kv) != 2 {
			return nil, nil, errors.New("invalid column")
		}
		lvs[kv[0]] = kv[1]
		keys = append(keys, kv[0])
	}
	return lvs, keys, nil
}

func (lr *LTSVRead) readline() (string, error) {
	for {
		line, _, err := lr.reader.ReadLine()
		if err != nil {
			return "", err
		}
		str := strings.TrimSpace(string(line))
		if len(str) != 0 {
			return str, nil
		}
	}
}
