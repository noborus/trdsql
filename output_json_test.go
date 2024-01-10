package trdsql

import (
	"reflect"
	"testing"
)

func Test_compatibleJSON(t *testing.T) {
	type args struct {
		v        any
		needNULL bool
		outNULL  string
	}
	tests := []struct {
		name string
		args args
		want any
	}{
		{
			name: "testText",
			args: args{"text", false, ""},
			want: "text",
		},
		{
			name: "testByte",
			args: args{[]byte{0xe3, 0x81, 0x82}, false, ""},
			want: "あ",
		},
		{
			name: "testInvalidByte",
			args: args{[]byte{0xef, 0xef, 0xef}, false, ""},
			want: `\xefefef`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compatibleJSON(tt.args.v, tt.args.needNULL, tt.args.outNULL); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valInterface() = %v, want %v", got, tt.want)
			}
		})
	}
}
