package trdsql

import (
	"io"
	"os"
)

// Writer is file format writer.
// Writer is a group of methods called from Export.
type Writer interface {
	PreWrite([]string, []string) error
	WriteRow([]interface{}, []string) error
	PostWrite() error
}

// WriteOpts is the option to determine the writer process.
type WriteOpts struct {
	OutFormat    Format
	OutDelimiter string
	OutHeader    bool
	OutStream    io.Writer
	ErrStream    io.Writer
}

type WriteOpt func(*WriteOpts)

func OutFormat(f Format) WriteOpt {
	return func(args *WriteOpts) {
		args.OutFormat = f
	}
}
func OutDelimiter(d string) WriteOpt {
	return func(args *WriteOpts) {
		args.OutDelimiter = d
	}
}
func OutHeader(h bool) WriteOpt {
	return func(args *WriteOpts) {
		args.OutHeader = h
	}
}
func OutStream(w io.Writer) WriteOpt {
	return func(args *WriteOpts) {
		args.OutStream = w
	}
}
func ErrStream(w io.Writer) WriteOpt {
	return func(args *WriteOpts) {
		args.ErrStream = w
	}
}

// NewWriter returns a Writer interface.
func NewWriter(options ...WriteOpt) Writer {
	writeOpts := &WriteOpts{
		OutFormat:    CSV,
		OutDelimiter: ",",
		OutHeader:    false,
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
	case CSV:
		return NewCSVWriter(writeOpts)
	default:
		return NewCSVWriter(writeOpts)
	}
}
