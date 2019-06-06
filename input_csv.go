package trdsql

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strconv"
)

// CSVRead provides methods of the Reader interface
type CSVRead struct {
	reader   *csv.Reader
	names    []string
	types    []string
	preRead  [][]string
	inHeader bool
}

func NewCSVReader(r io.Reader, opts ReadOpts) (Reader, error) {
	var err error

	if opts.InHeader {
		opts.InPreRead--
	}
	cr := &CSVRead{}
	cr.reader = csv.NewReader(r)
	cr.reader.LazyQuotes = true
	cr.reader.FieldsPerRecord = -1 // no check count
	cr.reader.TrimLeadingSpace = true
	cr.inHeader = opts.InHeader
	cr.reader.Comma, err = delimiter(opts.InDelimiter)

	if opts.InSkip > 0 {
		skip := make([]interface{}, 1)
		for i := 0; i < opts.InSkip; i++ {
			r, e := cr.ReadRow(skip)
			if e != nil {
				log.Printf("ERROR: skip error %s", e)
				break
			}
			debug.Printf("Skip row:%s\n", r)
		}
	}

	return cr, err
}

func delimiter(sepString string) (rune, error) {
	if sepString == "" {
		return 0, nil
	}
	sepRunes, err := strconv.Unquote(`'` + sepString + `'`)
	if err != nil {
		return ',', fmt.Errorf("can not get separator: %s:\"%s\"", err, sepString)
	}
	sepRune := ([]rune(sepRunes))[0]
	return sepRune, err
}

// GetColumn is reads the specified number of rows and determines the column name.
// The previously read row is stored in preRead.
func (cr *CSVRead) GetColumn(rowNum int) ([]string, error) {
	// Header
	if cr.inHeader {
		row, err := cr.reader.Read()
		if err != nil {
			return nil, err
		}
		cr.names = make([]string, len(row))
		for i, col := range row {
			if col == "" {
				cr.names[i] = "c" + strconv.Itoa(i+1)
			} else {
				cr.names[i] = col
			}
		}
	}

	for n := 0; n < rowNum; n++ {
		row, err := cr.reader.Read()
		if err != nil {
			return cr.names, err
		}
		rows := make([]string, len(row))
		for i, col := range row {
			rows[i] = col
			if len(cr.names) < i+1 {
				cr.names = append(cr.names, "c"+strconv.Itoa(i+1))
			}
		}
		cr.preRead = append(cr.preRead, rows)
	}
	return cr.names, nil
}

// GetTypes is reads the specified number of rows and determines the column type.
func (cr *CSVRead) GetTypes() ([]string, error) {
	cr.types = make([]string, len(cr.names))
	for i := 0; i < len(cr.names); i++ {
		cr.types[i] = DefaultDBType
	}
	return cr.types, nil
}

// PreReadRow is returns only columns that store preread rows.
func (cr *CSVRead) PreReadRow() [][]interface{} {
	rowNum := len(cr.preRead)
	rows := make([][]interface{}, rowNum)
	for n := 0; n < rowNum; n++ {
		rows[n] = make([]interface{}, len(cr.names))
		for i, f := range cr.preRead[n] {
			rows[n][i] = f
		}
	}
	return rows
}

// ReadRow is read the rest of the row.
func (cr *CSVRead) ReadRow(row []interface{}) ([]interface{}, error) {
	record, err := cr.reader.Read()
	if err != nil {
		return row, err
	}
	for i := 0; len(row) > i; i++ {
		if len(record) > i {
			row[i] = record[i]
		} else {
			row[i] = nil
		}
	}
	return row, nil
}
