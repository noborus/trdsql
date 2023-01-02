package trdsql

import (
	"io"
	"os"
)

// Writer is an interface that wraps the Write method that writes from the database to a file.
// Writer is a group of methods called from Export.
type Writer interface {
	// PreWrite is called first to write.
	// The arguments are a list of column names and a list of type names.
	PreWrite(columns []string, types []string) error
	// WriteRow is row write.
	WriteRow(row []interface{}, columns []string) error
	// PostWrite is called last in the write.
	PostWrite() error
}

// WriteOpts represents options that determine the behavior of the writer.
type WriteOpts struct {
	// OutStream is the output destination.
	OutStream io.Writer
	// ErrStream is the error output destination.
	ErrStream io.Writer

	// OutDelimiter is the output delimiter (Use only CSV and Raw).
	OutDelimiter string
	// OutQuote is the output quote character (Use only CSV).
	OutQuote string
	// OutNeedNULL is true, replace NULL with OutNULL.
	OutNULL string
	// OutFormat is the writing format.
	OutFormat Format
	// OutAllQuotes is true if Enclose all fields (Use only CSV).
	OutAllQuotes bool
	// True to use \r\n as the line terminator (Use only CSV).
	OutUseCRLF bool
	// OutHeader is true if it outputs a header(Use only CSV and Raw).
	OutHeader bool
	// OutNoWrap is true, do not wrap long columns(Use only AT and MD).
	OutNoWrap   bool
	OutNeedNULL bool
}

// WriteOpt is a function to set WriteOpts.
type WriteOpt func(*WriteOpts)

// OutFormat sets Format.
func OutFormat(f Format) WriteOpt {
	return func(args *WriteOpts) {
		args.OutFormat = f
	}
}

// OutDelimiter sets delimiter.
func OutDelimiter(d string) WriteOpt {
	return func(args *WriteOpts) {
		args.OutDelimiter = d
	}
}

// OutQuote sets quote.
func OutQuote(q string) WriteOpt {
	return func(args *WriteOpts) {
		args.OutQuote = q
	}
}

// OutUseCRLF sets use CRLF.
func OutUseCRLF(c bool) WriteOpt {
	return func(args *WriteOpts) {
		args.OutUseCRLF = c
	}
}

// OutAllQuotes sets all quotes.
func OutAllQuotes(a bool) WriteOpt {
	return func(args *WriteOpts) {
		args.OutAllQuotes = a
	}
}

// OutHeader sets flag to output header.
func OutHeader(h bool) WriteOpt {
	return func(args *WriteOpts) {
		args.OutHeader = h
	}
}

// OutNoWrap sets flag to output do not wrap long columns.
func OutNoWrap(w bool) WriteOpt {
	return func(args *WriteOpts) {
		args.OutNoWrap = w
	}
}

// OutNeedNULL sets a flag to replace NULL.
func OutNeedNULL(n bool) WriteOpt {
	return func(args *WriteOpts) {
		args.OutNeedNULL = n
	}
}

// OutNULL sets the output NULL string.
func OutNULL(s string) WriteOpt {
	return func(args *WriteOpts) {
		args.OutNULL = s
	}
}

// OutStream sets the output destination.
func OutStream(w io.Writer) WriteOpt {
	return func(args *WriteOpts) {
		args.OutStream = w
	}
}

// ErrStream sets the error output destination.
func ErrStream(w io.Writer) WriteOpt {
	return func(args *WriteOpts) {
		args.ErrStream = w
	}
}

// NewWriter returns a Writer interface.
// The argument is an option of Functional Option Pattern.
//
// usage:
//
//	NewWriter(
//		trdsql.OutFormat(trdsql.CSV),
//		trdsql.OutHeader(true),
//		trdsql.OutDelimiter(";"),
//	)
func NewWriter(options ...WriteOpt) Writer {
	writeOpts := &WriteOpts{
		OutFormat:    CSV,
		OutDelimiter: ",",
		OutQuote:     "\"",
		OutAllQuotes: false,
		OutUseCRLF:   false,
		OutHeader:    false,
		OutNeedNULL:  false,
		OutNULL:      "",
		OutStream:    os.Stdout,
		ErrStream:    os.Stderr,
	}
	for _, option := range options {
		option(writeOpts)
	}
	switch writeOpts.OutFormat {
	case LTSV:
		return NewLTSVWriter(writeOpts)
	case JSON:
		return NewJSONWriter(writeOpts)
	case RAW:
		return NewRAWWriter(writeOpts)
	case MD:
		return NewTWWriter(writeOpts, true)
	case AT:
		return NewTWWriter(writeOpts, false)
	case VF:
		return NewVFWriter(writeOpts)
	case TBLN:
		return NewTBLNWriter(writeOpts)
	case JSONL:
		return NewJSONLWriter(writeOpts)
	case CSV:
		return NewCSVWriter(writeOpts)
	default:
		return NewCSVWriter(writeOpts)
	}
}
