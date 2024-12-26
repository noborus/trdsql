package trdsql

import (
	"bufio"
	"io"
	"strings"
)

// TextReader provides a reader for text format.
type TextReader struct {
	reader *bufio.Reader
	num    int
	maxNum int
}

// NewTextReader returns a new TextReader.
func NewTextReader(reader io.Reader, opts *ReadOpts) (*TextReader, error) {
	r := &TextReader{
		reader: bufio.NewReader(reader),
	}

	if opts.InSkip > 0 {
		skipRead(r, opts.InSkip)
	}

	if opts.InLimitRead {
		r.maxNum = opts.InPreRead
	}
	return r, nil
}

// Names returns column names.
func (r *TextReader) Names() ([]string, error) {
	return []string{"text"}, nil
}

// Types returns column types.
func (r *TextReader) Types() ([]string, error) {
	return []string{"text"}, nil
}

// PreReadRow returns pre-read rows.
func (r *TextReader) PreReadRow() [][]any {
	return nil
}

// ReadRow reads a row.
func (r *TextReader) ReadRow() ([]any, error) {
	var builder strings.Builder
	for {
		if r.maxNum > 0 && r.num >= r.maxNum {
			return []any{""}, io.EOF
		}
		line, isPrefix, err := r.reader.ReadLine()
		if err != nil {
			return []any{""}, err
		}
		builder.Write(line)
		if isPrefix {
			continue
		}
		r.num++
		return []any{builder.String()}, nil
	}
}
