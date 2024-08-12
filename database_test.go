package trdsql

import (
	"context"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"
)

func TestConnect(t *testing.T) {
	type args struct {
		driver string
		dsn    string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "testSuccess",
			args:    args{driver: "sqlite", dsn: ""},
			wantErr: false,
		},
		{
			name:    "testFail",
			args:    args{driver: "sqlite2", dsn: ""},
			wantErr: true,
		},
		{
			name:    "testPostgres",
			args:    args{driver: "postgres", dsn: pgDsn()},
			wantErr: false,
		},
		{
			name:    "testMysql",
			args:    args{driver: "mysql", dsn: myDsn()},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Connect(tt.args.driver, tt.args.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestDB_Disconnect(t *testing.T) {
	type args struct {
		driver string
		dsn    string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "testSuccess",
			args:    args{driver: "sqlite", dsn: ""},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Connect(tt.args.driver, tt.args.dsn)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Disconnect(); (err != nil) != tt.wantErr {
				t.Errorf("DB.Disconnect() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_CreateTable(t *testing.T) {
	type fields struct {
		driver string
		dsn    string
	}
	type args struct {
		tableName   string
		names       []string
		types       []string
		isTemporary bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "testSuccess",
			fields: fields{driver: "sqlite", dsn: ""},
			args: args{
				tableName:   "test",
				names:       []string{"a", "b"},
				types:       []string{"text", "text"},
				isTemporary: true,
			},
			wantErr: false,
		},
		{
			name:   "testSuccess2",
			fields: fields{driver: "sqlite", dsn: ""},
			args: args{
				tableName:   "test",
				names:       []string{"c1"},
				types:       []string{"text"},
				isTemporary: false,
			},
			wantErr: false,
		},
		{
			name:   "testFail",
			fields: fields{driver: "sqlite", dsn: ""},
			args: args{
				tableName:   "test",
				names:       []string{},
				types:       []string{},
				isTemporary: true,
			},
			wantErr: true,
		},
		{
			name:   "testFail2",
			fields: fields{driver: "sqlite", dsn: ""},
			args: args{
				tableName:   "test",
				names:       []string{"c1"},
				types:       []string{},
				isTemporary: true,
			},
			wantErr: true,
		},
		{
			name:   "testFail3",
			fields: fields{driver: "sqlite", dsn: ""},
			args: args{
				tableName:   "test",
				names:       []string{"c1"},
				types:       []string{},
				isTemporary: false,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := Connect(tt.fields.driver, tt.fields.dsn)
			if err != nil {
				t.Fatal(err)
			}
			db.Tx, err = db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if err := db.CreateTable(ctx, tt.args.tableName, tt.args.names, tt.args.types, tt.args.isTemporary); (err != nil) != tt.wantErr {
				t.Errorf("DB.CreateTable() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := db.Tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := db.Disconnect(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDB_Select(t *testing.T) {
	type fields struct {
		driver string
		dsn    string
	}
	type args struct {
		query string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:    "testErr",
			fields:  fields{driver: "sqlite", dsn: ""},
			args:    args{query: "ERR"},
			wantErr: true,
		},
		{
			name:    "testErr2",
			fields:  fields{driver: "sqlite", dsn: ""},
			args:    args{query: "SELEC * FROM test"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := Connect(tt.fields.driver, tt.fields.dsn)
			if err != nil {
				t.Fatal(err)
			}
			db.Tx, err = db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.Select(ctx, tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("DB.Select() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			err = db.Tx.Commit()
			if err != nil {
				t.Fatal(err)
			}
			err = db.Disconnect()
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDB_Func(t *testing.T) {
	type fields struct {
		driver string
		dsn    string
	}
	type args struct {
		query string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:    "testsqliteVersion",
			fields:  fields{driver: "sqlite", dsn: ""},
			args:    args{query: "SELECT sqlite_version();"},
			wantErr: false,
		},
		{
			name:    "testsqlitefail",
			fields:  fields{driver: "sqlite", dsn: ""},
			args:    args{query: "SELECT repeat('f', 5);"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := Connect(tt.fields.driver, tt.fields.dsn)
			if err != nil {
				t.Fatal(err)
			}
			db.Tx, err = db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.Select(ctx, tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("DB.Select() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			err = db.Tx.Commit()
			if err != nil {
				t.Fatal(err)
			}
			err = db.Disconnect()
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDB_Import(t *testing.T) {
	type fields struct {
		driver string
		dsn    string
	}
	type args struct {
		tableName   string
		columnNames []string
		reader      Reader
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "testErr",
			fields: fields{driver: "sqlite", dsn: ""},
			args: args{
				tableName:   "test",
				columnNames: []string{"c1"},
				reader:      nil,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Connect(tt.fields.driver, tt.fields.dsn)
			if err != nil {
				t.Fatal(err)
			}
			db.Tx, err = db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := db.Import(ctx, tt.args.tableName, tt.args.columnNames, tt.args.reader); (err != nil) != tt.wantErr {
				t.Errorf("DB.Import() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := db.Tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := db.Disconnect(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDB_QuotedName(t *testing.T) {
	type fields struct {
		quote string
	}
	type args struct {
		oldName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name:   "test1",
			fields: fields{quote: "`"},
			args:   args{oldName: "test"},
			want:   "`test`",
		},
		{
			name:   "test2",
			fields: fields{quote: "\""},
			args:   args{oldName: "test"},
			want:   "\"test\"",
		},
		{
			name:   "test3",
			fields: fields{quote: "`"},
			args:   args{oldName: "`test`"},
			want:   "`test`",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				quote: tt.fields.quote,
			}
			if got := db.QuotedName(tt.args.oldName); got != tt.want {
				t.Errorf("DB.QuotedName() = %v, want %v", got, tt.want)
			}
		})
	}
}
