package trdsql

import (
	"fmt"
	"io"
)

// Reader is wrap the reader.
type Reader interface {
	Names() ([]string, error)
	Types() ([]string, error)
	PreReadRow() [][]interface{}
	ReadRow([]interface{}) ([]interface{}, error)
}

// ReadOpts option to determine reader.
type ReadOpts struct {
	// InFormat is read format.
	// The supported format is CSV/LTSV/JSON/TBLN.
	InFormat   Format
	realFormat Format

	// InPreRead is number of lines to read ahead.
	// CSV/LTSV reads the specified number of rows to
	// determine the number of columns.
	InPreRead int

	// InSkip is number of lines to skip.
	// Skip reading specified number of lines.
	InSkip int

	// InDelimiter is the field delimiter.
	// default is ','
	InDelimiter string

	// InHeader is true if there is a header.
	// It is used as a column name.
	InHeader bool

	// IsTemporary is a flag whether to make temporary table.
	// default is true.
	IsTemporary bool
}

// NewReadOpts Returns ReadOpts.
func NewReadOpts() *ReadOpts {
	return &ReadOpts{
		InFormat:    GUESS,
		InPreRead:   1,
		InSkip:      0,
		InDelimiter: ",",
		InHeader:    false,
		IsTemporary: true,
	}
}

// ReadOpt returns a *ReadOpts structure.
// Used when calling NewImporter.
// NewImporter(InFormat(CSV),InPreRead(2))
type ReadOpt func(*ReadOpts)

// InFormat is read format.
func InFormat(f Format) ReadOpt {
	return func(args *ReadOpts) {
		args.InFormat = f
	}
}

// InPreRead is number of lines to read ahead.
func InPreRead(p int) ReadOpt {
	return func(args *ReadOpts) {
		args.InPreRead = p
	}
}

// InSkip is number of lines to skip.
func InSkip(s int) ReadOpt {
	return func(args *ReadOpts) {
		args.InSkip = s
	}
}

// InDelimiter is the field delimiter.
func InDelimiter(d string) ReadOpt {
	return func(args *ReadOpts) {
		args.InDelimiter = d
	}
}

// InHeader is true if there is a header.
func InHeader(h bool) ReadOpt {
	return func(args *ReadOpts) {
		args.InHeader = h
	}
}

// IsTemporary is a flag whether to make temporary table.
func IsTemporary(t bool) ReadOpt {
	return func(args *ReadOpts) {
		args.IsTemporary = t
	}
}

// NewReader returns an Reader interface
// depending on the file to be imported.
func NewReader(reader io.Reader, readOpts *ReadOpts) (Reader, error) {
	switch readOpts.realFormat {
	case CSV:
		return NewCSVReader(reader, readOpts)
	case LTSV:
		return NewLTSVReader(reader, readOpts)
	case JSON:
		return NewJSONReader(reader, readOpts)
	case TBLN:
		return NewTBLNReader(reader)
	default:
		return nil, fmt.Errorf("unknown format")
	}
}
