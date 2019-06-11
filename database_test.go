package trdsql

import (
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
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
			args:    args{driver: "sqlite3", dsn: ""},
			wantErr: false,
		},
		{
			name:    "testFail",
			args:    args{driver: "sqlite2", dsn: ""},
			wantErr: true,
		},
		{
			name:    "testPostgres",
			args:    args{driver: "postgres", dsn: "dbname=trdsql_test"},
			wantErr: false,
		},
		{
			name:    "testMysql",
			args:    args{driver: "mysql", dsn: "root@/trdsql_test"},
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
			args:    args{driver: "sqlite3", dsn: ""},
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
			fields: fields{driver: "sqlite3", dsn: ""},
			args: args{
				tableName:   "test",
				names:       []string{"a", "b"},
				types:       []string{"text", "text"},
				isTemporary: true,
			},
			wantErr: false,
		},
		{
			name:   "testFail",
			fields: fields{driver: "sqlite3", dsn: ""},
			args: args{
				tableName:   "test",
				names:       []string{},
				types:       []string{},
				isTemporary: true,
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
			if err := db.CreateTable(tt.args.tableName, tt.args.names, tt.args.types, tt.args.isTemporary); (err != nil) != tt.wantErr {
				t.Errorf("DB.CreateTable() error = %v, wantErr %v", err, tt.wantErr)
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

func TestDB_Select(t *testing.T) {
	type fields struct {
		driver    string
		dsn       string
		escape    string
		rewritten []string
		maxBulk   int
		DB        *sql.DB
		Tx        *sql.Tx
	}
	type args struct {
		query string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *sql.Rows
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				driver:    tt.fields.driver,
				dsn:       tt.fields.dsn,
				escape:    tt.fields.escape,
				rewritten: tt.fields.rewritten,
				maxBulk:   tt.fields.maxBulk,
				DB:        tt.fields.DB,
				Tx:        tt.fields.Tx,
			}
			got, err := db.Select(tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("DB.Select() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DB.Select() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Import(t *testing.T) {
	type fields struct {
		driver    string
		dsn       string
		escape    string
		rewritten []string
		maxBulk   int
		DB        *sql.DB
		Tx        *sql.Tx
	}
	type args struct {
		tableName   string
		columnNames []string
		reader      Reader
		preRead     int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				driver:    tt.fields.driver,
				dsn:       tt.fields.dsn,
				escape:    tt.fields.escape,
				rewritten: tt.fields.rewritten,
				maxBulk:   tt.fields.maxBulk,
				DB:        tt.fields.DB,
				Tx:        tt.fields.Tx,
			}
			if err := db.Import(tt.args.tableName, tt.args.columnNames, tt.args.reader, tt.args.preRead); (err != nil) != tt.wantErr {
				t.Errorf("DB.Import() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_EscapeName(t *testing.T) {
	type fields struct {
		driver    string
		dsn       string
		escape    string
		rewritten []string
		maxBulk   int
		DB        *sql.DB
		Tx        *sql.Tx
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
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				driver:    tt.fields.driver,
				dsn:       tt.fields.dsn,
				escape:    tt.fields.escape,
				rewritten: tt.fields.rewritten,
				maxBulk:   tt.fields.maxBulk,
				DB:        tt.fields.DB,
				Tx:        tt.fields.Tx,
			}
			if got := db.EscapeName(tt.args.oldName); got != tt.want {
				t.Errorf("DB.EscapeName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_RewriteSQL(t *testing.T) {
	type fields struct {
		driver    string
		dsn       string
		escape    string
		rewritten []string
		maxBulk   int
		DB        *sql.DB
		Tx        *sql.Tx
	}
	type args struct {
		query   string
		oldName string
		newName string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantRewrite string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				driver:    tt.fields.driver,
				dsn:       tt.fields.dsn,
				escape:    tt.fields.escape,
				rewritten: tt.fields.rewritten,
				maxBulk:   tt.fields.maxBulk,
				DB:        tt.fields.DB,
				Tx:        tt.fields.Tx,
			}
			if gotRewrite := db.RewriteSQL(tt.args.query, tt.args.oldName, tt.args.newName); gotRewrite != tt.wantRewrite {
				t.Errorf("DB.RewriteSQL() = %v, want %v", gotRewrite, tt.wantRewrite)
			}
		})
	}
}
