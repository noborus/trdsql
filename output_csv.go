package trdsql

import (
	"bufio"
	"strings"
	"unicode"
	"unicode/utf8"
)

// CSVWriter provides methods of the Writer interface.
type CSVWriter struct {
	writer       *bufio.Writer
	outHeader    bool
	outDelimiter rune
	outQuote     rune
	outAllQuote  bool
	outUseCRLF   bool
	needQuotes   string
	endLine      string
}

// NewCSVWriter returns CSVWriter.
func NewCSVWriter(writeOpts *WriteOpts) *CSVWriter {
	w := &CSVWriter{}
	w.writer = bufio.NewWriter(writeOpts.OutStream)

	d, err := delimiter(writeOpts.OutDelimiter)
	if err != nil {
		debug.Printf("%s\n", err)
	}
	w.outDelimiter = d

	if len(writeOpts.OutQuote) > 0 {
		w.outQuote = ([]rune(writeOpts.OutQuote))[0]
	} else {
		w.outQuote = 0
	}
	w.outAllQuote = writeOpts.OutAllQuotes
	w.outUseCRLF = writeOpts.OutUseCRLF
	w.outHeader = writeOpts.OutHeader
	w.needQuotes = string(w.outDelimiter) + string(w.outQuote) + "\r\n"
	if writeOpts.OutUseCRLF {
		w.endLine = "\r\n"
	} else {
		w.endLine = "\n"
	}
	return w
}

// PreWrite is output of header and preparation.
func (w *CSVWriter) PreWrite(columns []string, types []string) error {
	if !w.outHeader {
		return nil
	}
	for n, column := range columns {
		if n > 0 {
			if _, err := w.writer.WriteRune(w.outDelimiter); err != nil {
				return err
			}
		}
		if err := w.writeColumn(column); err != nil {
			return err
		}
	}
	_, err := w.writer.WriteString(w.endLine)
	return err
}

// WriteRow is row write.
func (w *CSVWriter) WriteRow(values []interface{}, columns []string) error {
	for n, field := range values {
		if n > 0 {
			if _, err := w.writer.WriteRune(w.outDelimiter); err != nil {
				return err
			}
		}
		if err := w.writeColumn(ValString(field)); err != nil {
			return err
		}
	}
	_, err := w.writer.WriteString(w.endLine)
	return err
}

func (w *CSVWriter) writeColumn(column string) error {
	if !w.fieldNeedsQuotes(column) {
		_, err := w.writer.WriteString(column)
		return err
	}

	if _, err := w.writer.WriteRune(w.outQuote); err != nil {
		return err
	}
	var err error
	for _, r1 := range column {
		switch r1 {
		case w.outQuote:
			_, err = w.writer.WriteString(string([]rune{w.outQuote, w.outQuote}))
		case '\r':
			if !w.outUseCRLF {
				err = w.writer.WriteByte('\r')
			}
		case '\n':
			if w.outUseCRLF {
				_, err = w.writer.WriteString("\r\n")
			} else {
				err = w.writer.WriteByte('\n')
			}
		default:
			_, err = w.writer.WriteRune(r1)
		}
		if err != nil {
			return err
		}
	}
	_, err = w.writer.WriteRune(w.outQuote)
	return err
}

func (w *CSVWriter) fieldNeedsQuotes(field string) bool {
	if w.outAllQuote {
		return true
	}
	if field == "" {
		return false
	}
	if field == `\.` || strings.ContainsAny(field, w.needQuotes) {
		return true
	}
	r1, _ := utf8.DecodeRuneInString(field)
	return unicode.IsSpace(r1)
}

// PostWrite is flush.
func (w *CSVWriter) PostWrite() error {
	w.writer.Flush()
	return nil
}
