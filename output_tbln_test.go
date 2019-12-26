package trdsql

import "testing"

func Test_convertType(t *testing.T) {
	type args struct {
		dbType string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "testInt",
			args: args{"int"},
			want: "int",
		},
		{
			name: "testBigInt",
			args: args{"bigint"},
			want: "bigint",
		},
		{
			name: "testNumeric",
			args: args{"float"},
			want: "numeric",
		},
		{
			name: "testBool",
			args: args{"bool"},
			want: "bool",
		},
		{
			name: "testTimeStamp",
			args: args{"timestamp"},
			want: "timestamp",
		},
		{
			name: "testText",
			args: args{"text"},
			want: "text",
		},
		{
			name: "testUnknown",
			args: args{"unknown"},
			want: "text",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertType(tt.args.dbType); got != tt.want {
				t.Errorf("convertType() = %v, want %v", got, tt.want)
			}
		})
	}
}
