package trdsql

import (
	"io"
	"path/filepath"
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

func TestNewTBLNReaderFile(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		opts     *ReadOpts
		want     *TBLNRead
		wantErr  bool
	}{
		{
			name:     "test.tbln",
			fileName: "test.tbln",
			opts:     NewReadOpts(),
			want: &TBLNRead{
				preRead: [][]interface{}{
					{"1", "Bob"},
				},
			},
			wantErr: false,
		},
		{
			name:     "test.tbln2",
			fileName: "test.tbln",
			opts: NewReadOpts(
				InPreRead(2),
			),
			want: &TBLNRead{
				preRead: [][]interface{}{
					{"1", "Bob"},
					{"2", "Alice"},
				},
			},
			wantErr: false,
		},
		{
			name:     "test.tbln3",
			fileName: "test.tbln",
			opts: NewReadOpts(
				InPreRead(100),
			),
			want: &TBLNRead{
				preRead: [][]interface{}{
					{"1", "Bob"},
					{"2", "Alice"},
				},
			},
			wantErr: false,
		},
		{
			name:     "testNotNULL",
			fileName: "testnull.tbln",
			opts: NewReadOpts(
				InPreRead(3),
			),
			want: &TBLNRead{
				preRead: [][]interface{}{
					{"1", "Bob"},
					{"2", "Alice"},
					{"3", "NULL"},
				},
			},
			wantErr: false,
		},
		{
			name:     "testNULL",
			fileName: "testnull.tbln",
			opts: NewReadOpts(
				InPreRead(3),
				InNeedNULL(true),
				InNULL("NULL"),
			),
			want: &TBLNRead{
				preRead: [][]interface{}{
					{"1", "Bob"},
					{"2", "Alice"},
					{"3", nil},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := singleFileOpen(filepath.Join(dataDir, tt.fileName))
			if err != nil {
				t.Error(err)
			}
			got, err := NewTBLNReader(file, tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewTBLNReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewTBLNReader().preRead = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}
