package trdsql

import (
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestNewJSONPATHReader(t *testing.T) {
	type args struct {
		reader io.Reader
		opts   *ReadOpts
	}
	tests := []struct {
		name    string
		args    args
		want    *JSONPATHReader
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				reader: strings.NewReader(`[{"c1":"1","c2":"Orange"},{"c1":"2","c2":"Melon"},{"c1":"3","c2":"Apple"}]`),
				opts:   NewReadOpts(),
			},
			want: &JSONPATHReader{
				names:   []string{"c1", "c2"},
				preRead: []map[string]string{{"c1": "1", "c2": "Orange"}, {"c1": "2", "c2": "Melon"}, {"c1": "3", "c2": "Apple"}},
			},
			wantErr: false,
		},
		{
			name: "testPath",
			args: args{
				reader: strings.NewReader(`[{"c1":"1","c2":"Orange"},{"c1":"2","c2":"Melon"},{"c1":"3","c2":"Apple"}]`),
				opts:   NewReadOpts(InPath("0")),
			},
			want: &JSONPATHReader{
				names:   []string{"c1", "c2"},
				preRead: []map[string]string{{"c1": "1", "c2": "Orange"}},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewJSONPATHReader(tt.args.reader, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewJSONPATHReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !arraySortEqual(t, got.names, tt.want.names) {
				t.Errorf("NewJSONPATHReader() = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewJSONPATHReader() = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}
