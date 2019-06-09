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

func NewLTSVReader(reader io.Reader, opts ReadOpts) (Reader, error) {
	r := &LTSVRead{}
	r.reader = bufio.NewReader(reader)
	r.delimiter = "\t"

	if opts.InSkip > 0 {
		skip := make([]interface{}, 1)
		for i := 0; i < opts.InSkip; i++ {
			row, err := r.ReadRow(skip)
			if err != nil {
				log.Printf("ERROR: skip error %s", err)
				break
			}
			debug.Printf("Skip row:%s\n", row)
		}
	}

	return r, nil
}

// GetColumn is reads the specified number of rows and determines the column name.
// The previously read row is stored in preRead.
func (r *LTSVRead) GetColumn(rowNum int) ([]string, error) {
	names := map[string]bool{}
	for i := 0; i < rowNum; i++ {
		row, keys, err := r.read()
		if err != nil {
			return r.names, err
		}
		// Add only unique column names.
		for k := 0; k < len(keys); k++ {
			if !names[keys[k]] {
				names[keys[k]] = true
				r.names = append(r.names, keys[k])
			}
		}
		r.preRead = append(r.preRead, row)
	}
	return r.names, nil
}

// GetTypes is reads the specified number of rows and determines the column type.
func (r *LTSVRead) GetTypes() ([]string, error) {
	r.types = make([]string, len(r.names))
	for i := 0; i < len(r.names); i++ {
		r.types[i] = DefaultDBType
	}
	return r.types, nil
}

// PreReadRow is returns only columns that store preread rows.
func (r *LTSVRead) PreReadRow() [][]interface{} {
	rowNum := len(r.preRead)
	rows := make([][]interface{}, rowNum)
	for n := 0; n < rowNum; n++ {
		rows[n] = make([]interface{}, len(r.names))
		for i := range r.names {
			rows[n][i] = r.preRead[n][r.names[i]]
		}
	}
	return rows
}

// ReadRow is read the rest of the row.
func (r *LTSVRead) ReadRow(row []interface{}) ([]interface{}, error) {
	record, _, err := r.read()
	if err != nil {
		return row, err
	}
	for i := range r.names {
		row[i] = record[r.names[i]]
	}
	return row, nil
}

func (r *LTSVRead) read() (map[string]string, []string, error) {
	line, err := r.readline()
	if err != nil {
		return nil, nil, err
	}
	columns := strings.Split(line, r.delimiter)
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

func (r *LTSVRead) readline() (string, error) {
	for {
		line, _, err := r.reader.ReadLine()
		if err != nil {
			return "", err
		}
		str := strings.TrimSpace(string(line))
		if len(str) != 0 {
			return str, nil
		}
	}
}
