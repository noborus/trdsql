package trdsql

import (
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestNewYAMLReader(t *testing.T) {
	type args struct {
		reader io.Reader
		opts   *ReadOpts
	}
	tests := []struct {
		name    string
		args    args
		want    *YAMLReader
		wantErr bool
	}{
		{
			name: "testBlank",
			args: args{
				reader: strings.NewReader(""),
				opts:   NewReadOpts(),
			},
			want: &YAMLReader{
				names:   nil,
				preRead: nil,
			},
			wantErr: false,
		},
		{
			name: "testString",
			args: args{
				reader: strings.NewReader("a: 1\nb: test"),
				opts:   NewReadOpts(),
			},
			want: &YAMLReader{
				names:   []string{"a", "b"},
				preRead: []map[string]any{{"a": "1", "b": "test"}},
			},
			wantErr: false,
		},
		{
			name: "test1",
			args: args{
				reader: strings.NewReader(`
- c1: "1"
  c2: Orange
- c1: "2"
  c2: Melon
- c1: "3"
  c2: Apple
`),
				opts: NewReadOpts(),
			},
			want: &YAMLReader{
				names:   []string{"c1", "c2"},
				preRead: []map[string]any{{"c1": "1", "c2": "Orange"}, {"c1": "2", "c2": "Melon"}, {"c1": "3", "c2": "Apple"}},
			},
			wantErr: false,
		},
		{
			name: "testArray",
			args: args{
				reader: strings.NewReader(`
- "a"
- "b"
- "c"
`),
				opts: NewReadOpts(),
			},
			want: &YAMLReader{
				names:   []string{"c1"},
				preRead: []map[string]any{{"c1": "a"}, {"c1": "b"}, {"c1": "c"}},
			},
			wantErr: false,
		},
		{
			name: "testJQ",
			args: args{
				reader: strings.NewReader(`
- c1: "1"
  c2: Orange
- c1: "2"
  c2: Melon
- c1: "3"
  c2: Apple
`),
				opts: NewReadOpts(InJQ(".[0]")),
			},
			want: &YAMLReader{
				names:   []string{"c1", "c2"},
				preRead: []map[string]any{{"c1": "1", "c2": "Orange"}},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewYAMLReader(tt.args.reader, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewYAMLReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !arraySortEqual(t, got.names, tt.want.names) {
				t.Errorf("NewYAMLReader() = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewJSONReader() = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}
