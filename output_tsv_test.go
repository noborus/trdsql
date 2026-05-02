//nolint:goconst
package trdsql

import (
	"bytes"
	"testing"
)

func TestNewTSVWriter(t *testing.T) {
	tests := []struct {
		name       string
		writeOpts  WriteOpts
		wantEnd    string
		wantHeader bool
	}{
		{
			name: "default",
			writeOpts: WriteOpts{
				OutStream: new(bytes.Buffer),
			},
			wantEnd:    "\n",
			wantHeader: false,
		},
		{
			name: "crlf",
			writeOpts: WriteOpts{
				OutStream:  new(bytes.Buffer),
				OutUseCRLF: true,
			},
			wantEnd:    "\r\n",
			wantHeader: false,
		},
		{
			name: "header",
			writeOpts: WriteOpts{
				OutStream: new(bytes.Buffer),
				OutHeader: true,
			},
			wantEnd:    "\n",
			wantHeader: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewTSVWriter(&tt.writeOpts)
			if w.endLine != tt.wantEnd {
				t.Errorf("endLine = %q, want %q", w.endLine, tt.wantEnd)
			}
			if w.outHeader != tt.wantHeader {
				t.Errorf("outHeader = %v, want %v", w.outHeader, tt.wantHeader)
			}
		})
	}
}

func TestTSVWriter_PreWrite(t *testing.T) {
	tests := []struct {
		name      string
		writeOpts WriteOpts
		columns   []string
		types     []string
		want      string
		wantErr   bool
	}{
		{
			name: "noHeader",
			writeOpts: WriteOpts{
				OutHeader: false,
			},
			columns: []string{"c1", "c2"},
			types:   []string{"text", "text"},
			want:    "",
		},
		{
			name: "header",
			writeOpts: WriteOpts{
				OutHeader: true,
			},
			columns: []string{"c1", "c2"},
			types:   []string{"text", "text"},
			want:    "c1\tc2\n",
		},
		{
			name: "headerCRLF",
			writeOpts: WriteOpts{
				OutHeader:  true,
				OutUseCRLF: true,
			},
			columns: []string{"c1", "c2"},
			types:   []string{"text", "text"},
			want:    "c1\tc2\r\n",
		},
		{
			name: "headerSanitize",
			writeOpts: WriteOpts{
				OutHeader: true,
			},
			columns: []string{"col\tone", "col\ntwo"},
			types:   []string{"text", "text"},
			want:    "col one\tcol two\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			tt.writeOpts.OutStream = buf
			w := NewTSVWriter(&tt.writeOpts)
			if err := w.PreWrite(tt.columns, tt.types); (err != nil) != tt.wantErr {
				t.Errorf("PreWrite() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := w.PostWrite(); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("PreWrite() output = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTSVWriter_WriteRow(t *testing.T) {
	tests := []struct {
		name      string
		writeOpts WriteOpts
		values    []any
		want      string
		wantErr   bool
	}{
		{
			name:      "simple",
			writeOpts: WriteOpts{},
			values:    []any{"a", "b"},
			want:      "a\tb\n",
		},
		{
			name:      "tabInField",
			writeOpts: WriteOpts{},
			values:    []any{"val\t1", "ok"},
			want:      "val 1\tok\n",
		},
		{
			name:      "newlineInField",
			writeOpts: WriteOpts{},
			values:    []any{"line1\nline2", "ok"},
			want:      "line1 line2\tok\n",
		},
		{
			name:      "crlfInField",
			writeOpts: WriteOpts{},
			values:    []any{"line1\r\nline2", "ok"},
			want:      "line1\r line2\tok\n",
		},
		{
			name:      "bareCRInField",
			writeOpts: WriteOpts{},
			values:    []any{"val\r1", "ok"},
			want:      "val\r1\tok\n",
		},
		{
			name:      "quoteInField",
			writeOpts: WriteOpts{},
			values:    []any{`say "hello"`, "ok"},
			want:      "say \"hello\"\tok\n",
		},
		{
			name: "nullValue",
			writeOpts: WriteOpts{
				OutNeedNULL: true,
				OutNULL:     "NULL",
			},
			values: []any{nil, "ok"},
			want:   "NULL\tok\n",
		},
		{
			name:      "nullNoReplace",
			writeOpts: WriteOpts{},
			values:    []any{nil, "ok"},
			want:      "\tok\n",
		},
		{
			name: "crlf",
			writeOpts: WriteOpts{
				OutUseCRLF: true,
			},
			values: []any{"a", "b"},
			want:   "a\tb\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			tt.writeOpts.OutStream = buf
			w := NewTSVWriter(&tt.writeOpts)
			if err := w.PreWrite(nil, nil); err != nil {
				t.Fatal(err)
			}
			if err := w.WriteRow(tt.values, nil); (err != nil) != tt.wantErr {
				t.Errorf("WriteRow() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := w.PostWrite(); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("WriteRow() output = %q, want %q", got, tt.want)
			}
		})
	}
}
