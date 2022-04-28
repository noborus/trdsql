package trdsql

import (
	"testing"
	"time"
)

func TestValString(t *testing.T) {
	type args struct {
		v interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test1",
			args: args{v: "test"},
			want: "test",
		},
		{
			name: "testTime",
			args: args{v: time.Date(2020, 1, 3, 17, 28, 18, 0, time.UTC)},
			want: "2020-01-03T17:28:18Z",
		},
		{
			name: "testByte",
			args: args{v: []byte("test")},
			want: "test",
		},
		{
			name: "testByteHex",
			args: args{v: []byte("\xf3\xf2\xff")},
			want: "\\xf3f2ff",
		},
		{
			name: "testNil",
			args: args{v: nil},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValString(tt.args.v); got != tt.want {
				t.Errorf("ValString() = %v, want %v", got, tt.want)
			}
		})
	}
}
