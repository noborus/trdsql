package cmd

import (
	"github.com/noborus/trdsql"
	"io"
	"strings"
	"testing"
)

func Test_configOpen(t *testing.T) {
	tests := []struct {
		name    string
		appName string
		args    string
		want    bool
	}{
		{
			name:    "noFile",
			appName: "err",
			args:    "",
			want:    true,
		},
		{
			name:    "errFile",
			appName: "err",
			args:    "noFile",
			want:    true,
		},
	}
	for _, tt := range tests {
		trdsql.AppName = tt.appName
		t.Run(tt.name, func(t *testing.T) {
			if got := configOpen(tt.args); !((got == nil) == tt.want) {
				t.Errorf("configOpen() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_loadConfig(t *testing.T) {
	tests := []struct {
		name    string
		args    io.Reader
		want    string
		wantErr bool
	}{
		{
			name:    "nil",
			args:    nil,
			want:    "",
			wantErr: true,
		},
		{
			name:    "err",
			args:    strings.NewReader(`err`),
			want:    "",
			wantErr: true,
		},
		{
			name: "test1",
			args: strings.NewReader(`
			{"db": "sample",
			 "database": {
				   "sample": {
						"driver": "sqlite3",
						"dns": ":memory:"
					}
				}
			}
			`),
			want:    "sample",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadConfig(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("loadConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.Db != tt.want {
				t.Errorf("loadConfig() = %v, want %v", got.Db, tt.want)
			}
		})
	}
}
