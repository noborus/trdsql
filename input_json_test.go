package trdsql

import (
	"strings"
	"testing"
)

func TestNewJSONReader(t *testing.T) {
	tests := []struct {
		name    string
		stream  string
		wantErr bool
	}{
		{
			name:    "testErr",
			stream:  "t",
			wantErr: true,
		},
		{
			name:    "test1",
			stream:  "{}",
			wantErr: false,
		},
		{
			name:    "test2",
			stream:  `[{"c1":"1","c2":"Orange"},{"c1":"2","c2":"Melon"},{"c1":"3","c2":"Apple"}]`,
			wantErr: false,
		},
		{
			name: "test3",
			stream: `
{"c1":"1","c2":"Orange"}
{"c1":"2","c2":"Melon"}
{"c1":"3","c2":"Apple"}
`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(tt.stream)
			_, err := NewJSONReader(r, NewReadOpts())
			if (err != nil) != tt.wantErr {
				t.Errorf("NewJSONReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
