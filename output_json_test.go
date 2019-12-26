package trdsql

import (
	"reflect"
	"testing"
)

func Test_valInterface(t *testing.T) {
	type args struct {
		v interface{}
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "testText",
			args: args{"text"},
			want: "text",
		},
		{
			name: "testByte",
			args: args{[]byte{0xe3, 0x81, 0x82}},
			want: "あ",
		},
		{
			name: "testInvalidByte",
			args: args{[]byte{0xef, 0xef, 0xef}},
			want: `\xefefef`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := valInterface(tt.args.v); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valInterface() = %v, want %v", got, tt.want)
			}
		})
	}
}
