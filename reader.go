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
	InFormat    Format
	realFormat  Format
	InPreRead   int
	InSkip      int
	InDelimiter string
	InHeader    bool
	IsTemporary bool
}

type ReadOpt func(*ReadOpts)

func InFormat(f Format) ReadOpt {
	return func(args *ReadOpts) {
		args.InFormat = f
	}
}
func InPreRead(p int) ReadOpt {
	return func(args *ReadOpts) {
		args.InPreRead = p
	}
}
func InSkip(s int) ReadOpt {
	return func(args *ReadOpts) {
		args.InSkip = s
	}
}
func InDelimiter(d string) ReadOpt {
	return func(args *ReadOpts) {
		args.InDelimiter = d
	}
}
func InHeader(h bool) ReadOpt {
	return func(args *ReadOpts) {
		args.InHeader = h
	}
}
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
