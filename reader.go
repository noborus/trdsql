package trdsql

import (
	"io"
	"log"
	"sync"
)

// extToFormat is a map of file extensions to formats.
var extToFormat map[string]Format = map[string]Format{
	"CSV":   CSV,
	"LTSV":  LTSV,
	"JSON":  JSON,
	"JSONL": JSON,
	"YAML":  YAML,
	"YML":   YAML,
	"TBLN":  TBLN,
	"TSV":   TSV,
	"PSV":   PSV,
	"WIDTH": WIDTH,
	"TEXT":  TEXT,
}

// ReaderFunc is a function that creates a new Reader.
type ReaderFunc func(io.Reader, *ReadOpts) (Reader, error)

// readerFuncs maps formats to their corresponding ReaderFunc.
var readerFuncs = map[Format]ReaderFunc{
	CSV: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewCSVReader(reader, opts)
	},
	LTSV: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewLTSVReader(reader, opts)
	},
	JSON: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewJSONReader(reader, opts)
	},
	YAML: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewYAMLReader(reader, opts)
	},
	TBLN: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewTBLNReader(reader, opts)
	},
	TSV: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewTSVReader(reader, opts)
	},
	PSV: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewPSVReader(reader, opts)
	},
	WIDTH: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewGWReader(reader, opts)
	},
	TEXT: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewTextReader(reader, opts)
	},
}

var (
	// extFormat is the next format number to be assigned.
	extFormat Format = 100
	// registerMux is a mutex to protect access to the register.
	registerMux = &sync.Mutex{}
)

func RegisterReaderFunc(ext string, readerFunc ReaderFunc) {
	registerMux.Lock()
	defer registerMux.Unlock()
	extToFormat[ext] = extFormat
	readerFuncs[extFormat] = readerFunc
	extFormat++
}

// Reader is wrap the reader.
// Reader reads from tabular files.
type Reader interface {
	// Names returns column names.
	Names() ([]string, error)
	// Types returns column types.
	Types() ([]string, error)
	// PreReadRow is returns only columns that store preRead rows.
	PreReadRow() [][]any
	// ReadRow is read the rest of the row.
	ReadRow() ([]any, error)
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

	// InRowNumber is a flag whether to add row number.
	InRowNumber bool
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
		InRowNumber: false,
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

// IsTemporary is a flag whether to make temporary table.
func IsTemporary(t bool) ReadOpt {
	return func(args *ReadOpts) {
		args.IsTemporary = t
	}
}

// InRowNumber is a flag whether to add line number.
func InRowNumber(t bool) ReadOpt {
	return func(args *ReadOpts) {
		args.InRowNumber = t
	}
}

// NewReader returns an Reader interface
// depending on the file to be imported.
func NewReader(reader io.Reader, readOpts *ReadOpts) (Reader, error) {
	if reader == nil {
		return nil, ErrNoReader
	}
	readerFunc, ok := readerFuncs[readOpts.realFormat]
	if !ok {
		return nil, ErrUnknownFormat
	}

	return readerFunc(reader, readOpts)
}

func skipRead(r Reader, skipNum int) {
	for i := 0; i < skipNum; i++ {
		row, err := r.ReadRow()
		if err != nil {
			log.Printf("ERROR: skip error %s", err)
			break
		}
		debug.Printf("Skip row:%s\n", row)
	}
}
