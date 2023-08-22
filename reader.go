package trdsql

import (
	"io"
	"log"
)

// Reader is wrap the reader.
// Reader reads from tabular files.
type Reader interface {
	// Names returns column names.
	Names() ([]string, error)
	// Types returns column types.
	Types() ([]string, error)
	// PreReadRow is returns only columns that store preread rows.
	PreReadRow() [][]interface{}
	// ReadRow is read the rest of the row.
	ReadRow(row []interface{}) ([]interface{}, error)
}

// ReadOpts represents options that determine the behavior of the reader.
type ReadOpts struct {
	// InDelimiter is the field delimiter.
	// default is ','
	InDelimiter string

	// InNULL is a string to replace with NULL.
	InNULL string

	// InJQuery is a jq expression.
	InJQuery string

	// InFormat is read format.
	// The supported format is CSV/LTSV/JSON/TBLN.
	InFormat   Format
	realFormat Format

	// InPreRead is number of rows to read ahead.
	// CSV/LTSV reads the specified number of rows to
	// determine the number of columns.
	InPreRead int

	// InSkip is number of rows to skip.
	// Skip reading specified number of lines.
	InSkip int

	// InLimitRead is limit read.
	InLimitRead bool

	// InHeader is true if there is a header.
	// It is used as a column name.
	InHeader bool
	// InNeedNULL is true, replace InNULL with NULL.
	InNeedNULL bool

	// IsTemporary is a flag whether to make temporary table.
	// default is true.
	IsTemporary bool

	// InYAMLToJSON is true, convert YAML to JSON.
	InYAMLToJSON bool
}

// NewReadOpts Returns ReadOpts.
func NewReadOpts(options ...ReadOpt) *ReadOpts {
	readOpts := &ReadOpts{
		InFormat:    GUESS,
		InPreRead:   1,
		InLimitRead: false,
		InSkip:      0,
		InDelimiter: ",",
		InHeader:    false,
		IsTemporary: true,
		InJQuery:    "",
		InNeedNULL:  false,
		InNULL:      "",
	}
	for _, option := range options {
		option(readOpts)
	}
	return readOpts
}

// ReadOpt returns a *ReadOpts structure.
// Used when calling NewImporter.
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

func InLimitRead(p bool) ReadOpt {
	return func(args *ReadOpts) {
		args.InLimitRead = p
	}
}

// InJQ is jq expression.
func InJQ(p string) ReadOpt {
	return func(args *ReadOpts) {
		args.InJQuery = p
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

// InNeedNULL sets a flag as to whether it should be replaced with NULL.
func InNeedNULL(n bool) ReadOpt {
	return func(args *ReadOpts) {
		args.InNeedNULL = n
	}
}

// In NULL is a string to replace with NULL.
func InNULL(s string) ReadOpt {
	return func(args *ReadOpts) {
		args.InNULL = s
	}
}

// InYAMLToJSON is true, convert YAML to JSON.
func InYAMLToJSON(t bool) ReadOpt {
	return func(args *ReadOpts) {
		args.InYAMLToJSON = t
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
	if reader == nil {
		return nil, ErrNoReader
	}
	switch readOpts.realFormat {
	case CSV:
		return NewCSVReader(reader, readOpts)
	case TSV:
		readOpts.InDelimiter = "\t"
		return NewCSVReader(reader, readOpts)
	case PSV:
		readOpts.InDelimiter = "|"
		return NewCSVReader(reader, readOpts)
	case LTSV:
		return NewLTSVReader(reader, readOpts)
	case JSON:
		return NewJSONReader(reader, readOpts)
	case YAML:
		return NewYAMLReader(reader, readOpts)
	case TBLN:
		return NewTBLNReader(reader, readOpts)
	case WIDTH:
		return NewGWReader(reader, readOpts)
	default:
		return nil, ErrUnknownFormat
	}
}

func skipRead(r Reader, skipNum int) {
	skip := make([]interface{}, 1)
	for i := 0; i < skipNum; i++ {
		row, err := r.ReadRow(skip)
		if err != nil {
			log.Printf("ERROR: skip error %s", err)
			break
		}
		debug.Printf("Skip row:%s\n", row)
	}
}
