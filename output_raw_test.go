package trdsql

import (
	"bytes"
	"reflect"
	"testing"
)

func TestNewRAWWriter(t *testing.T) {
	outStream := new(bytes.Buffer)
	type args struct {
		writeOpts *WriteOpts
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "testDefault",
			args: args{
				writeOpts: &WriteOpts{
					OutDelimiter: ",",
					OutStream:    outStream,
				},
			},
			want: ",",
		},
		{
			name: "testMultiDelimiter",
			args: args{
				writeOpts: &WriteOpts{
					OutDelimiter: "|||",
					OutStream:    outStream,
				},
			},
			want: "|||",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewRAWWriter(tt.args.writeOpts)
			if !reflect.DeepEqual(got.delimiter, tt.want) {
				t.Errorf("NewCSVWriter() = %v, want %v", got.delimiter, tt.want)
			}
		})
	}
}

func TestRAWWriter_PreWrite(t *testing.T) {
	type args struct {
		columns []string
		types   []string
	}
	tests := []struct {
		name      string
		writeOpts WriteOpts
		args      args
		want      []string
		wantErr   bool
	}{
		{
			name: "empty",
			writeOpts: WriteOpts{
				OutDelimiter: ",",
			},
			args: args{
				columns: []string{},
				types:   []string{},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "emptyHeader",
			writeOpts: WriteOpts{
				OutDelimiter: ",",
				OutHeader:    true,
			},
			args: args{
				columns: []string{"h1", "h2"},
				types:   []string{"text", "text"},
			},
			want:    make([]string, 2),
			wantErr: false,
		},
		{
			name: "noHeader",
			writeOpts: WriteOpts{
				OutDelimiter: ",",
				OutHeader:    false,
			},
			args: args{
				columns: []string{"v1", "v2"},
				types:   []string{"text", "text"},
			},
			want:    make([]string, 2),
			wantErr: false,
		},
		{
			name: "multiDelimiter",
			writeOpts: WriteOpts{
				OutDelimiter: "||",
				OutHeader:    false,
			},
			args: args{
				columns: []string{"v1", "v2"},
				types:   []string{"text", "text"},
			},
			want:    make([]string, 2),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewRAWWriter(&tt.writeOpts)
			if err := w.PreWrite(tt.args.columns, tt.args.types); (err != nil) != tt.wantErr {
				t.Errorf("RAWWriter.PreWrite() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
