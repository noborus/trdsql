package cmd

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/noborus/trdsql"
)

func Test_inputFormat(t *testing.T) {
	type args struct {
		i inputFlag
	}
	tests := []struct {
		name string
		args args
		want trdsql.Format
	}{
		{
			name: "testCSV",
			args: args{
				i: inputFlag{
					CSV: true,
				},
			},
			want: trdsql.CSV,
		},
		{
			name: "testTBLN",
			args: args{
				i: inputFlag{
					TBLN: true,
				},
			},
			want: trdsql.TBLN,
		},
		{
			name: "testGUESS",
			args: args{
				i: inputFlag{},
			},
			want: trdsql.GUESS,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := inputFormat(tt.args.i); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("inputFormat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_outputFormat(t *testing.T) {
	type args struct {
		o outputFlag
	}
	tests := []struct {
		name string
		args args
		want trdsql.Format
	}{
		{
			name: "testCSV",
			args: args{
				o: outputFlag{
					CSV: true,
				},
			},
			want: trdsql.CSV,
		},
		{
			name: "testTBLN",
			args: args{
				o: outputFlag{
					TBLN: true,
				},
			},
			want: trdsql.TBLN,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := outputFormat(tt.args.o); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("outputFormat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getQuery(t *testing.T) {
	type argss struct {
		args     []string
		fileName string
	}
	tests := []struct {
		name    string
		argss   argss
		want    string
		wantErr bool
	}{
		{
			name: "testARGS",
			argss: argss{
				[]string{"SELECT 1"},
				"",
			},
			want:    "SELECT 1",
			wantErr: false,
		},
		{
			name: "testARGS2",
			argss: argss{
				[]string{"SELECT", "1"},
				"",
			},
			want:    "SELECT 1",
			wantErr: false,
		},
		{
			name: "testFile",
			argss: argss{
				[]string{},
				filepath.Join("..", "testdata", "test.sql"),
			},
			want:    "SELECT * FROM testdata/test.csv\n",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getQuery(tt.argss.args, tt.argss.fileName)
			if (err != nil) != tt.wantErr {
				t.Errorf("getQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getDB(t *testing.T) {
	type args struct {
		cfg     *config
		cDB     string
		cDriver string
		cDSN    string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getDB(tt.args.cfg, tt.args.cDB, tt.args.cDriver, tt.args.cDSN)
			if got != tt.want {
				t.Errorf("getDB() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("getDB() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_getCommand(t *testing.T) {
	type args struct {
		args []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getCommand(tt.args.args); got != tt.want {
				t.Errorf("getCommand() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_quotedArg(t *testing.T) {
	type args struct {
		arg string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quotedArg(tt.args.arg); got != tt.want {
				t.Errorf("quotedArg() = %v, want %v", got, tt.want)
			}
		})
	}
}
