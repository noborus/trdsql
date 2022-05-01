package trdsql

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNewAnalyzeOpts(t *testing.T) {
	tests := []struct {
		name string
		want *AnalyzeOpts
	}{
		{
			name: "test",
			want: &AnalyzeOpts{
				Command:   AppName,
				Quote:     "\\`",
				Detail:    true,
				OutStream: os.Stdout,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewAnalyzeOpts(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewAnalyzeOpts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnalyze(t *testing.T) {
	type args struct {
		fileName string
		opts     *AnalyzeOpts
		readOpts *ReadOpts
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "empty",
			args: args{
				fileName: filepath.Join(dataDir, ""),
				opts: &AnalyzeOpts{
					Command:   AppName,
					Quote:     "\\`",
					Detail:    true,
					OutStream: new(bytes.Buffer),
				},
				readOpts: NewReadOpts(),
			},
			wantErr: true,
		},
		{
			name: "testNoFile",
			args: args{
				fileName: filepath.Join(dataDir, "nofile"),
				opts: &AnalyzeOpts{
					Command:   AppName,
					Quote:     "\\`",
					Detail:    true,
					OutStream: new(bytes.Buffer),
				},
				readOpts: NewReadOpts(),
			},
			wantErr: true,
		},
		{
			name: "test",
			args: args{
				fileName: filepath.Join(dataDir, "test.csv"),
				opts: &AnalyzeOpts{
					Command:   AppName,
					Quote:     "\\`",
					Detail:    true,
					OutStream: new(bytes.Buffer),
				},
				readOpts: NewReadOpts(),
			},
			wantErr: false,
		},
		{
			name: "invalidDelimiter",
			args: args{
				fileName: filepath.Join(dataDir, "test.csv"),
				opts: &AnalyzeOpts{
					Command:   AppName,
					Quote:     "\\`",
					Detail:    true,
					OutStream: new(bytes.Buffer),
				},
				readOpts: NewReadOpts(InDelimiter("~")),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Analyze(tt.args.fileName, tt.args.opts, tt.args.readOpts); (err != nil) != tt.wantErr {
				t.Errorf("Analyze() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_examples(t *testing.T) {
	type args struct {
		tableName string
		names     []string
		results   []string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "test",
			args: args{
				tableName: "test",
				names:     []string{"id", "name"},
				results:   []string{"1", "ttt"},
			},
			want: []string{
				"SELECT id, name FROM test",
				"SELECT id, name FROM test WHERE id = '1'",
				"SELECT id, count(id) FROM test GROUP BY id",
				"SELECT id, name FROM test ORDER BY id LIMIT 10",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := examples(tt.args.tableName, tt.args.names, tt.args.results); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("examples() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_quoted(t *testing.T) {
	type args struct {
		name  string
		quote string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "nonQuote",
			args: args{
				name:  "non",
				quote: `"`,
			},
			want: `non`,
		},
		{
			name: "Quote",
			args: args{
				name:  "Quote",
				quote: `"`,
			},
			want: `"Quote"`,
		},
		{
			name: "backSlash",
			args: args{
				name:  "Quote",
				quote: "`",
			},
			want: "`Quote`",
		},
		{
			name: "keyWord",
			args: args{
				name:  "select",
				quote: `"`,
			},
			want: `"select"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quoted(tt.args.name, tt.args.quote); got != tt.want {
				t.Errorf("quoted() = %v, want %v", got, tt.want)
			}
		})
	}
}
