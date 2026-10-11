package trdsql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func TestWriters_NULLReplacement(t *testing.T) {
	columns := []string{"integer", "real", "zero", "false_value", "true_value", "missing", "text"}
	values := []any{int64(42), float64(2.5), int64(0), false, true, nil, "text"}
	for _, format := range []struct {
		name      string
		newWriter func(*WriteOpts) Writer
		want      string
	}{
		{
			name:      "json",
			newWriter: func(opts *WriteOpts) Writer { return NewJSONWriter(opts) },
			want:      `[{"integer":42,"real":2.5,"zero":0,"false_value":false,"true_value":true,"missing":%s,"text":"text"}]`,
		},
		{
			name:      "jsonl",
			newWriter: func(opts *WriteOpts) Writer { return NewJSONLWriter(opts) },
			want:      `{"integer":42,"real":2.5,"zero":0,"false_value":false,"true_value":true,"missing":%s,"text":"text"}`,
		},
		{
			name:      "yaml",
			newWriter: func(opts *WriteOpts) Writer { return NewYAMLWriter(opts) },
			want:      "- integer: 42\n  real: 2.5\n  zero: 0\n  false_value: false\n  true_value: true\n  missing: %s\n  text: text\n",
		},
	} {
		t.Run(format.name, func(t *testing.T) {
			for _, replacement := range []struct {
				name    string
				enabled bool
				value   string
				want    string
			}{
				{name: "default", want: "null"},
				{name: "custom", enabled: true, value: "(NULL)", want: `"(NULL)"`},
				{name: "empty", enabled: true, want: `""`},
			} {
				t.Run(replacement.name, func(t *testing.T) {
					var output bytes.Buffer
					writer := format.newWriter(&WriteOpts{
						OutStream:   &output,
						OutNeedNULL: replacement.enabled,
						OutNULL:     replacement.value,
					})
					if err := writer.PreWrite(columns, nil); err != nil {
						t.Fatal(err)
					}
					if err := writer.WriteRow(values, columns); err != nil {
						t.Fatal(err)
					}
					if err := writer.PostWrite(); err != nil {
						t.Fatal(err)
					}
					null := replacement.want
					if format.name == "yaml" {
						if replacement.enabled && replacement.value != "" {
							null = replacement.value
						}
					} else {
						var compact bytes.Buffer
						if err := json.Compact(&compact, output.Bytes()); err != nil {
							t.Fatal(err)
						}
						output = compact
					}
					want := fmt.Sprintf(format.want, null)
					if got := output.String(); got != want {
						t.Errorf("output = %q, want %q", got, want)
					}
				})
			}
		})
	}
}
