package trdsql

import (
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestNewLTSVReader(t *testing.T) {
	type args struct {
		reader io.Reader
		opts   *ReadOpts
	}
	tests := []struct {
		name    string
		args    args
		want    *LTSVReader
		wantErr bool
	}{
		{
			name: "empty",
			args: args{
				reader: strings.NewReader(""),
				opts:   NewReadOpts(),
			},
			want: &LTSVReader{
				names:   nil,
				types:   nil,
				preRead: nil,
			},
			wantErr: false,
		},
		{
			name: "oneLine",
			args: args{
				reader: strings.NewReader("ID:1\tname:test"),
				opts:   NewReadOpts(),
			},
			want: &LTSVReader{
				names:   []string{"ID", "name"},
				types:   []string{"text", "text"},
				preRead: []map[string]string{{"ID": "1", "name": "test"}},
			},
			wantErr: false,
		},
		{
			name: "invalidLTSV",
			args: args{
				reader: strings.NewReader("ID;1\tname:test"),
				opts:   NewReadOpts(),
			},
			want: &LTSVReader{
				names:   nil,
				types:   nil,
				preRead: nil,
			},
			wantErr: true,
		},
		{
			name: "diffColumn",
			args: args{
				reader: strings.NewReader("ID:1\tname:test\nID:2\tvalue:test"),
				opts:   NewReadOpts(InPreRead(2)),
			},
			want: &LTSVReader{
				names: []string{"ID", "name", "value"},
				types: []string{"text", "text", "text"},
				preRead: []map[string]string{
					{"ID": "1", "name": "test"},
					{"ID": "2", "value": "test"},
				},
			},
			wantErr: false,
		},
		{
			name: "ignoreColumn",
			args: args{
				reader: strings.NewReader("ID:1\tname:test\nID:2\tvalue:test"),
				opts:   NewReadOpts(InPreRead(1)),
			},
			want: &LTSVReader{
				names:   []string{"ID", "name"},
				types:   []string{"text", "text"},
				preRead: []map[string]string{{"ID": "1", "name": "test"}},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLTSVReader(tt.args.reader, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLTSVReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.names, tt.want.names) {
				t.Errorf("NewLTSVReader().names = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.types, tt.want.types) {
				t.Errorf("NewLTSVReader().types = %v, want %v", got.types, tt.want.types)
			}
			if !reflect.DeepEqual(got.preRead, tt.want.preRead) {
				t.Errorf("NewLTSVReader().preRead = %v, want %v", got.preRead, tt.want.preRead)
			}
		})
	}
}

func TestLtsvFile(t *testing.T) {
	file, err := singleFileOpen("testdata/test.ltsv")
	if err != nil {
		t.Error(err)
	}
	lr, err := NewLTSVReader(file, NewReadOpts())
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.Names()
	if err != nil {
		t.Error(`Names error`)
	}
	if len(list) != 3 {
		t.Error(`invalid column`)
	}
}

func TestIndefiniteLtsvFile1(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.ltsv")
	if err != nil {
		t.Error(err)
	}
	lr, err := NewLTSVReader(file, NewReadOpts())
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.Names()
	if err != nil {
		t.Error(`Names error`)
	}
	if len(list) != 3 {
		t.Error(`invalid column`)
	}
}

func TestIndefiniteLtsvFile2(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.ltsv")
	if err != nil {
		t.Error(err)
	}
	ro := NewReadOpts()
	ro.InPreRead = 2
	lr, err := NewLTSVReader(file, ro)
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.Names()
	if err != nil {
		t.Error(`Names error`)
	}
	if len(list) != 4 {
		t.Error(`invalid column`)
	}
}

func TestIndefiniteLtsvFile3(t *testing.T) {
	file, err := singleFileOpen("testdata/test_indefinite.ltsv")
	if err != nil {
		t.Error(err)
	}
	ro := NewReadOpts()
	ro.InPreRead = 100
	lr, err := NewLTSVReader(file, ro)
	if err != nil {
		t.Error(`NewLTSVReader error`)
	}
	list, err := lr.Names()
	if err != nil && err != io.EOF {
		t.Error(err)
	}
	if len(list) != 5 {
		t.Error(`invalid column`)
	}
}
