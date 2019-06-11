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

// NewReader returns an Reader interface
// depending on the file to be imported.
func NewReader(reader io.Reader, opts ReadOpts) (Reader, error) {
	switch opts.InFormat {
	case CSV:
		return NewCSVReader(reader, opts)
	case LTSV:
		return NewLTSVReader(reader, opts)
	case JSON:
		return NewJSONReader(reader, opts)
	case TBLN:
		return NewTBLNReader(reader)
	default:
		return nil, fmt.Errorf("unknown format")
	}
}
