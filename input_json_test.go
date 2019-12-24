package trdsql

import (
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func arraySortEqual(t *testing.T, a []string, b []string) bool {
	t.Helper()
	if len(a) != len(b) {
		return false
	}

	a_copy := make([]string, len(a))
	b_copy := make([]string, len(b))

	copy(a_copy, a)
	copy(b_copy, b)

	sort.Strings(a_copy)
	sort.Strings(b_copy)

	return reflect.DeepEqual(a_copy, b_copy)
}

func TestNewJSONReader(t *testing.T) {
	type args struct {
		reader io.Reader
		opts   *ReadOpts
	}
	tests := []struct {
		name    string
		args    args
		want    *JSONReader
		wantErr bool
	}{
		{
			name: "empty",
			args: args{
				reader: strings.NewReader(""),
				opts:   NewReadOpts(),
			},
			want: &JSONReader{
				names:   nil,
				preRead: nil,
			},
			wantErr: false,
		},
		{
			name: "invalidJSON",
			args: args{
				reader: strings.NewReader("t"),
				opts:   NewReadOpts(),
			},
			want: &JSONReader{
				names:   nil,
				preRead: nil,
			},
			wantErr: true,
		},
		{
			name: "emptyJSON",
			args: args{
				reader: strings.NewReader("{}"),
				opts:   NewReadOpts(),
			},
			want: &JSONReader{
				names:   nil,
				preRead: []map[string]string{{}},
			},
			wantErr: false,
		},
		{
			name: "test1",
			args: args{
				reader: strings.NewReader(`[{"c1":"1","c2":"Orange"},{"c1":"2","c2":"Melon"},{"c1":"3","c2":"Apple"}]`),
				opts:   NewReadOpts(),
			},
			want: &JSONReader{
				names:   []string{"c1", "c2"},
				preRead: []map[string]string{{"c1": "1", "c2": "Orange"}},
			},
			wantErr: false,
		},
		{
			name: "test2",
			args: args{
				reader: strings.NewReader(`
{"c1":"1","c2":"Orange"}
{"c1":"2","c2":"Melon"}
{"c1":"3","c2":"Apple"}`),
				opts: NewReadOpts(),
			},
			want: &JSONReader{
				names:   []string{"c1", "c2"},
				preRead: []map[string]string{{"c1": "1", "c2": "Orange"}},
			},
			wantErr: false,
		},
		{
			name: "testArray",
			args: args{
				reader: strings.NewReader(`[["a"],["b"]]`),
				opts:   NewReadOpts(),
			},
			want: &JSONReader{
				names:   []string{"c1"},
				preRead: []map[string]string{{"c1": "[\"a\"]"}},
			},
			wantErr: false,
		},
		{
			name: "testObject",
			args: args{
				reader: strings.NewReader(`{"a":"b"}`),
				opts:   NewReadOpts(),
			},
			want: &JSONReader{
				names:   []string{"a"},
				preRead: []map[string]string{{"a": "b"}},
			},
			wantErr: false,
		},
		{
			name: "testArray2",
			args: args{
				reader: strings.NewReader(`[["a","b"],["c","d"]]`),
				opts:   NewReadOpts(),
			},
			want: &JSONReader{
				names:   []string{"c1"},
				preRead: []map[string]string{{"c1": "[\"a\",\"b\"]"}},
			},
			wantErr: false,
		},
		{
			name: "diffColumn",
			args: args{
				reader: strings.NewReader(`
{"id":"1","name":"Orange"}
{"id":"2","name":"Melon"}
{"id":"3","name":"Apple"}
{"id":"4","name":"Banana","color":"Yellow"}`),
				opts: NewReadOpts(),
			},
			want: &JSONReader{
				names:   []string{"id", "name"},
				preRead: []map[string]string{{"id": "1", "name": "Orange"}},
			},
			wantErr: false,
		},
		{
			name: "diffColumn2",
			args: args{
				reader: strings.NewReader(`
{"id":"1","name":"Orange"}
{"id":"2","name":"Melon"}
{"id":"3","name":"Apple"}
{"id":"4","name":"Banana","color":"Yellow"}`),
				opts: NewReadOpts(InPreRead(5)),
			},
			want: &JSONReader{
				names: []string{"id", "name", "color"},
				preRead: []map[string]string{
					{"id": "1", "name": "Orange"},
					{"id": "2", "name": "Melon"},
					{"id": "3", "name": "Apple"},
					{"id": "4", "name": "Banana", "color": "Yellow"},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewJSONReader(tt.args.reader, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewJSONReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !arraySortEqual(t, got.names, tt.want.names) {
				t.Errorf("NewJSONReader().names = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewJSONReader().preRead = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}
