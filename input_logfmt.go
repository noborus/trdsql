package trdsql

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

// LogfmtReader parses logfmt (space-separated key=value pairs).
// The value may be a bare token or a double-quoted string.
// See https://brandur.org/logfmt for the convention.
type LogfmtReader struct {
	reader    *bufio.Reader
	inNULL    string
	preRead   []map[string]string
	names     []string
	types     []string
	limitRead bool
	needNULL  bool
}

// NewLogfmtReader returns a LogfmtReader configured with input options.
func NewLogfmtReader(reader io.Reader, opts *ReadOpts) (*LogfmtReader, error) {
	r := &LogfmtReader{}
	r.reader = bufio.NewReader(reader)

	if opts.InSkip > 0 {
		skipRead(r, opts.InSkip)
	}

	r.limitRead = opts.InLimitRead

	r.needNULL = opts.InNeedNULL
	r.inNULL = opts.InNULL

	names := map[string]bool{}
	for i := 0; i < opts.InPreRead; i++ {
		row, keys, err := r.read()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return r, err
			}
			r.setColumnType()
			debug.Print(err.Error())
			return r, nil
		}

		// Add only unique column names.
		for _, k := range keys {
			if !names[k] {
				names[k] = true
				r.names = append(r.names, k)
			}
		}
		r.preRead = append(r.preRead, row)
	}
	r.setColumnType()
	return r, nil
}

func (r *LogfmtReader) setColumnType() {
	if r.names == nil {
		return
	}
	r.types = make([]string, len(r.names))
	for i := 0; i < len(r.names); i++ {
		r.types[i] = DefaultDBType
	}
}

// Names returns column names.
func (r *LogfmtReader) Names() ([]string, error) {
	return r.names, nil
}

// Types returns column types.
// All logfmt types return the DefaultDBType.
func (r *LogfmtReader) Types() ([]string, error) {
	return r.types, nil
}

// PreReadRow returns only columns that store preread rows.
func (r *LogfmtReader) PreReadRow() [][]any {
	rowNum := len(r.preRead)
	rows := make([][]any, rowNum)
	for n := range rowNum {
		rows[n] = make([]any, len(r.names))
		for i := range r.names {
			rows[n][i] = r.preRead[n][r.names[i]]
			if r.needNULL {
				rows[n][i] = replaceNULL(r.inNULL, rows[n][i])
			}
		}
	}
	return rows
}

// ReadRow reads the next row.
func (r *LogfmtReader) ReadRow(row []any) ([]any, error) {
	if r.limitRead {
		return nil, io.EOF
	}

	record, _, err := r.read()
	if err != nil {
		return row, err
	}
	for i, name := range r.names {
		row[i] = record[name]
		if r.needNULL {
			row[i] = replaceNULL(r.inNULL, row[i])
		}
	}
	return row, nil
}

func (r *LogfmtReader) read() (map[string]string, []string, error) {
	line, err := r.readline()
	if err != nil {
		return nil, nil, err
	}
	return parseLogfmt(line)
}

func (r *LogfmtReader) readline() (string, error) {
	var builder strings.Builder
	for {
		line, isPrefix, err := r.reader.ReadLine()
		if err != nil {
			return "", err
		}
		builder.Write(line)
		if isPrefix {
			continue
		}
		str := strings.TrimSpace(builder.String())
		if len(str) != 0 {
			return str, nil
		}
		builder.Reset()
	}
}

// parseLogfmt parses a single logfmt line into ordered keys and their values.
// A key without an "=" (a bare flag) is stored with an empty value.
func parseLogfmt(line string) (map[string]string, []string, error) {
	lvs := make(map[string]string)
	keys := make([]string, 0)
	i := 0
	n := len(line)
	for i < n {
		// Skip separating whitespace.
		for i < n && isLogfmtSpace(line[i]) {
			i++
		}
		if i >= n {
			break
		}

		// Read the key up to '=' or whitespace.
		start := i
		for i < n && line[i] != '=' && !isLogfmtSpace(line[i]) {
			i++
		}
		key := line[start:i]
		if key == "" {
			// A stray '='; skip it so parsing makes progress.
			i++
			continue
		}

		value := ""
		if i < n && line[i] == '=' {
			i++ // consume '='
			if i < n && line[i] == '"' {
				v, next, err := readQuoted(line, i)
				if err != nil {
					return nil, nil, ErrInvalidColumn
				}
				value = v
				i = next
			} else {
				vStart := i
				for i < n && !isLogfmtSpace(line[i]) {
					i++
				}
				value = line[vStart:i]
			}
		}

		if _, ok := lvs[key]; !ok {
			keys = append(keys, key)
		}
		lvs[key] = value
	}
	return lvs, keys, nil
}

func isLogfmtSpace(b byte) bool {
	return b == ' ' || b == '\t'
}

// readQuoted reads a double-quoted value starting at s[i] (which must be '"')
// and returns the unquoted value and the index just past the closing quote.
func readQuoted(s string, i int) (string, int, error) {
	j := i + 1
	for j < len(s) {
		switch s[j] {
		case '\\':
			j += 2
		case '"':
			j++
			v, err := strconv.Unquote(s[i:j])
			if err != nil {
				return "", 0, err
			}
			return v, j, nil
		default:
			j++
		}
	}
	// Unterminated quote.
	return "", 0, ErrInvalidColumn
}
