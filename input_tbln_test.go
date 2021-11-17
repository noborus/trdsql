package trdsql

import (
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestNewTBLNReader(t *testing.T) {
	type args struct {
		reader io.Reader
	}
	tests := []struct {
		name      string
		args      args
		want      *TBLNRead
		wantNames []string
		wantTypes []string
		wantErr   bool
	}{
		{
			name: "empty",
			args: args{
				reader: strings.NewReader(""),
			},
			want: &TBLNRead{
				preRead: nil,
			},
			wantNames: nil,
			wantTypes: nil,
			wantErr:   false,
		},
		{
			name: "noHeader",
			args: args{
				reader: strings.NewReader("| 1 | test |"),
			},
			want: &TBLNRead{
				preRead: [][]interface{}{{"1", "test"}},
			},
			wantNames: []string{"c1", "c2"},
			wantTypes: []string{"text", "text"},
			wantErr:   false,
		},
		{
			name: "noNameHeader",
			args: args{
				reader: strings.NewReader("; type: | int | text |\n| 1 | test |"),
			},
			want: &TBLNRead{
				preRead: [][]interface{}{{"1", "test"}},
			},
			wantNames: []string{"c1", "c2"},
			wantTypes: []string{"int", "text"},
			wantErr:   false,
		},
		{
			name: "noTypeHeader",
			args: args{
				reader: strings.NewReader("; name: | id | name |\n| 1 | test |"),
			},
			want: &TBLNRead{
				preRead: [][]interface{}{{"1", "test"}},
			},
			wantNames: []string{"id", "name"},
			wantTypes: []string{"text", "text"},
			wantErr:   false,
		},
		{
			name: "diffNameType",
			args: args{
				reader: strings.NewReader("; name: | id | name |\ntype: | int |\n| 1 | test |"),
			},
			want: &TBLNRead{
				preRead: nil,
			},
			wantNames: []string{"id", "name"},
			wantTypes: nil,
			wantErr:   true,
		},
		{
			name: "oneRow",
			args: args{
				reader: strings.NewReader("; name: | id | name |\n; type: | int | text |\n| 1 | test |"),
			},
			want: &TBLNRead{
				preRead: [][]interface{}{{"1", "test"}},
			},
			wantNames: []string{"id", "name"},
			wantTypes: []string{"int", "text"},
			wantErr:   false,
		},
		{
			name: "errRow",
			args: args{
				reader: strings.NewReader("; name: | id | name |\n; type: | int | text |\n******"),
			},
			want: &TBLNRead{
				preRead: nil,
			},
			wantNames: []string{"id", "name"},
			wantTypes: []string{"int", "text"},
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ro := NewReadOpts()
			got, err := NewTBLNReader(tt.args.reader, ro)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewTBLNReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotNames, _ := got.Names()
			if !reflect.DeepEqual(gotNames, tt.wantNames) {
				t.Errorf("NewTBLNReader().Names() = %v, want %v", gotNames, tt.wantNames)
			}
			gotTypes, _ := got.Types()
			if !reflect.DeepEqual(gotTypes, tt.wantTypes) {
				t.Errorf("NewTBLNReader().Types() = %v, want %v", gotTypes, tt.wantTypes)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewTBLNReader().preRead = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTBLNFile(t *testing.T) {
	file, err := singleFileOpen("testdata/test.tbln")
	want := [][]interface{}{{"1", "Bob"}}
	if err != nil {
		t.Error(err)
	}
	ro := NewReadOpts()
	tr, err := NewTBLNReader(file, ro)
	if err != nil {
		t.Error(`tblnInputNew error`)
	}
	list, err := tr.Names()
	if err != nil {
		t.Error(`Names error`)
	}
	if len(list) != 2 {
		t.Error(`invalid column`)
	}
	got := tr.PreReadRow()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("TBLN file %v, want %v", got, want)
	}
}
