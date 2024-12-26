package trdsql

import (
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNewGWReader(t *testing.T) {
	type args struct {
		reader io.Reader
		opts   *ReadOpts
	}
	tests := []struct {
		name    string
		args    args
		want    *GWReader
		wantErr bool
	}{
		{
			name: "empty",
			args: args{
				reader: strings.NewReader(""),
				opts:   NewReadOpts(),
			},
			want: &GWReader{
				names: nil,
				types: nil,
			},
			wantErr: false,
		},
		{
			name: "ps",
			args: args{
				reader: strings.NewReader(`    PID TTY          TIME CMD
	914367 pts/2    00:00:04 zsh
   1051667 pts/2    00:00:00 ps
`),
				opts: NewReadOpts(),
			},
			want: &GWReader{
				names: []string{"PID", "TTY", "TIME", "CMD"},
				types: []string{"text", "text", "text", "text"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewGWReader(tt.args.reader, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewGWReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.names, tt.want.names) {
				t.Errorf("NewGWVReader().names = %v, want %v", got.names, tt.want.names)
			}
			if !reflect.DeepEqual(got.types, tt.want.types) {
				t.Errorf("NewGWReader().types = %v, want %v", got.types, tt.want.types)
			}
		})
	}
}

func TestGWReader_ReadRow(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		opts     *ReadOpts

		want    []any
		wantErr bool
	}{
		{
			name:     "ps",
			fileName: "ps.txt",
			opts:     NewReadOpts(),
			want: []any{
				"root", "1", "0.0", "0.0", "168720", "13812", "?", "Ss", "Mar11", "1:11", "/sbin/init splash",
			},
			wantErr: false,
		},
		{
			name:     "dpkg",
			fileName: "dpkg.txt",
			opts: NewReadOpts(
				InSkip(3),
			),
			want: []any{
				"ii", "accountsservice", "22.07.5-2ubuntu1.3", "amd64", "query and manipulate user account information",
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
			r, err := NewGWReader(file, tt.opts)
			if err != nil {
				t.Error(err)
			}
			got, err := r.ReadRow()
			if (err != nil) != tt.wantErr {
				t.Errorf("GWReader.ReadRow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GWReader.ReadRow() = \n%#v, want \n%#v", got, tt.want)
			}
		})
	}
}
