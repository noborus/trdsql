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

	copyA := make([]string, len(a))
	copyB := make([]string, len(b))

	copy(copyA, a)
	copy(copyB, b)

	sort.Strings(copyA)
	sort.Strings(copyB)

	return reflect.DeepEqual(copyA, copyB)
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
				preRead: []map[string]interface{}{{}},
			},
			wantErr: false,
		},
		{
			name: "test1",
			args: args{
				reader: strings.NewReader(`[{"c1":"1","c2":"Orange"},{"c1":"2","c2":"Melon"},{"c1":"3","c2":"Apple"}]`),
				opts:   NewReadOpts(InPreRead(3)),
			},
			want: &JSONReader{
				names:   []string{"c1", "c2"},
				preRead: []map[string]interface{}{{"c1": "1", "c2": "Orange"}, {"c1": "2", "c2": "Melon"}, {"c1": "3", "c2": "Apple"}},
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
				preRead: []map[string]interface{}{{"c1": "1", "c2": "Orange"}},
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
				preRead: []map[string]interface{}{{"c1": "[\"a\"]"}, {"c1": "[\"b\"]"}},
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
				preRead: []map[string]interface{}{{"c1": "[\"a\",\"b\"]"}, {"c1": "[\"c\",\"d\"]"}},
			},
			wantErr: false,
		},
		{
			name: "testArray3",
			args: args{
				reader: strings.NewReader(`["a","b"]`),
				opts:   NewReadOpts(),
			},
			want: &JSONReader{
				names:   []string{"c1"},
				preRead: []map[string]interface{}{{"c1": "a"}, {"c1": "b"}},
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
				preRead: []map[string]interface{}{{"a": "b"}},
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
				preRead: []map[string]interface{}{{"id": "1", "name": "Orange"}},
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
				preRead: []map[string]interface{}{
					{"id": "1", "name": "Orange"},
					{"id": "2", "name": "Melon"},
					{"id": "3", "name": "Apple"},
					{"id": "4", "name": "Banana", "color": "Yellow"},
				},
			},
			wantErr: false,
		},
		{
			name: "testJQ",
			args: args{
				reader: strings.NewReader(`[{"c1":"1","c2":"Orange"},{"c1":"2","c2":"Melon"},{"c1":"3","c2":"Apple"}]`),
				opts:   NewReadOpts(InJQ(".[0]")),
			},
			want: &JSONReader{
				names:   []string{"c1", "c2"},
				preRead: []map[string]interface{}{{"c1": "1", "c2": "Orange"}},
			},
			wantErr: false,
		},
		{
			name: "testJQ2",
			args: args{
				reader: strings.NewReader(`{"employees":[
						{"name":"Shyam", "email":"shyamjaiswal@gmail.com"},
						{"name":"Bob", "email":"bob32@gmail.com"},
						{"name":"Jai", "email":"jai87@gmail.com"}
					]}`),
				opts: NewReadOpts(InJQ(".employees")),
			},
			want: &JSONReader{
				names: []string{"name", "email"},
				preRead: []map[string]interface{}{
					{"name": "Shyam", "email": "shyamjaiswal@gmail.com"},
					{"name": "Bob", "email": "bob32@gmail.com"},
					{"name": "Jai", "email": "jai87@gmail.com"},
				},
			},
			wantErr: false,
		},
		{
			name: "testJQ3",
			args: args{
				reader: strings.NewReader(`{"menu": {
						"id": "file",
						"value": "File",
						"popup": {
						  "menuitem": [
							{"value": "New", "onclick": "CreateDoc()"},
							{"value": "Open", "onclick": "OpenDoc()"},
							{"value": "Save", "onclick": "SaveDoc()"}
						  ]
						}
					  }}`),
				opts: NewReadOpts(InJQ(`".menu.popup.menuitem"`)),
			},
			want: &JSONReader{
				names: []string{"value", "onclick"},
				preRead: []map[string]interface{}{
					{"value": "New", "onclick": "CreateDoc()"},
					{"value": "Open", "onclick": "OpenDoc()"},
					{"value": "Save", "onclick": "SaveDoc()"},
				},
			},
			wantErr: false,
		},
		{
			name: "testJQ4",
			args: args{
				reader: strings.NewReader(`[{"id":1},{"id":2},{"id":3}]`),
				opts:   NewReadOpts(InJQ(".")),
				// opts:   NewReadOpts(InJQ(`. as {$a} ?// [$a] ?// $a | $a`)),
			},
			want: &JSONReader{
				names: []string{"id"},
				preRead: []map[string]interface{}{
					{"id": "1"},
					{"id": "2"},
					{"id": "3"},
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
				t.Errorf("NewJSONReader() = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewJSONReader() = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}

func TestNewJSONReaderWithNULL(t *testing.T) {
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
			name: "testNULL1",
			args: args{
				reader: strings.NewReader(`[{"id":1},{"id":null},{"id":3}]`),
				opts:   NewReadOpts(),
			},
			want: &JSONReader{
				names: []string{"id"},
				preRead: []map[string]interface{}{
					{"id": "1"},
					{"id": nil},
					{"id": "3"},
				},
			},
			wantErr: false,
		},
		{
			name: "testNULL2",
			args: args{
				reader: strings.NewReader(`[{"id":1},{"id":"N"},{"id":3}]`),
				opts: NewReadOpts(
					InNeedNULL(true),
					InNULL("N"),
				),
			},
			want: &JSONReader{
				names: []string{"id"},
				preRead: []map[string]interface{}{
					{"id": "1"},
					{"id": nil},
					{"id": "3"},
				},
			},
			wantErr: false,
		},
		{
			name: "testNULL3",
			args: args{
				reader: strings.NewReader(`[{"c1":"1","c2":null},{"c1":"2","c2":"\\N"},{"c1":"3","c2":"Apple"}]`),
				opts: NewReadOpts(
					InNeedNULL(true),
					InNULL("\\N"),
				),
			},
			want: &JSONReader{
				names:   []string{"c1", "c2"},
				preRead: []map[string]interface{}{{"c1": "1", "c2": nil}, {"c1": "2", "c2": nil}, {"c1": "3", "c2": "Apple"}},
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
				t.Errorf("NewJSONReader() = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewJSONReader() = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}
