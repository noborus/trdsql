package trdsql

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNewImporter(t *testing.T) {
	type args struct {
		inputFormat Format
	}
	tests := []struct {
		name string
		args args
		want Format
	}{
		{
			name: "test1",
			args: args{inputFormat: CSV},
			want: CSV,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewImporter(InFormat(CSV)); !reflect.DeepEqual(got.ReadOpts.InFormat, tt.want) {
				t.Errorf("NewImporter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestImporter_Import(t *testing.T) {
	type fields struct {
		ReadOpts *ReadOpts
	}
	tests := []struct {
		name    string
		fields  fields
		query   string
		want    string
		wantErr bool
	}{
		{
			name:    "test1",
			fields:  fields{ReadOpts: NewReadOpts()},
			query:   "SELECT 1",
			want:    "SELECT 1",
			wantErr: false,
		},
		{
			name:    "testNoFile",
			fields:  fields{ReadOpts: NewReadOpts()},
			query:   "SELECT * FROM ttttnofile.csv",
			want:    "SELECT * FROM ttttnofile.csv",
			wantErr: false,
		},
		{
			name:    "testGenerate_series",
			fields:  fields{ReadOpts: NewReadOpts()},
			query:   "SELECT * FROM generate_series(1,10)",
			want:    "SELECT * FROM generate_series(1,10)",
			wantErr: false,
		},
		{
			name:    "testGenerate_series2",
			fields:  fields{ReadOpts: NewReadOpts()},
			query:   "SELECT * FROM generate_series(1,10,2)",
			want:    "SELECT * FROM generate_series(1,10,2)",
			wantErr: false,
		},
		{
			name:    "test3",
			fields:  fields{ReadOpts: NewReadOpts(InDelimiter("ddd"))},
			query:   "SELECT * FROM testdata/test.csv",
			want:    "SELECT * FROM testdata/test.csv",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Connect("sqlite3", "")
			if err != nil {
				t.Fatal(err)
			}
			db.Tx, err = db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			i := &ReadFormat{
				ReadOpts: tt.fields.ReadOpts,
			}
			got, err := i.Import(db, tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Importer.Import() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Importer.Import() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_listTable(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantTbl map[string]string
		wantIdx []int
	}{
		{
			name:    "noTable",
			query:   "SELECT 1;",
			wantTbl: map[string]string{},
			wantIdx: []int{},
		},
		{
			name:    "testTable",
			query:   "SELECT * FROM test;",
			wantTbl: map[string]string{"test": "test"},
			wantIdx: []int{6},
		},
		{
			name:    "testJoin",
			query:   "SELECT test.a FROM test LEFT JOIN test2 ON (test.b = test2.b);",
			wantTbl: map[string]string{"test": "test", "test2": "test2"},
			wantIdx: []int{6, 12},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTbl, gotIdx := TableNames(SQLFields(tt.query))
			if !reflect.DeepEqual(gotTbl, tt.wantTbl) {
				t.Errorf("TableNames() Table Map = %v, want %v", gotTbl, tt.wantTbl)
			}
			if !reflect.DeepEqual(gotIdx, tt.wantIdx) {
				t.Errorf("TableNames() Table Index = %v, want %v", gotIdx, tt.wantIdx)
			}
		})
	}
}

func Test_SQLFields(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "testNo",
			args: args{query: ""},
			want: []string{},
		},
		{
			name: "testDoubleQuote",
			args: args{query: `SELECT * FROM "C:\file with a space.csv"`},
			want: []string{`SELECT`, ` `, `*`, ` `, `FROM`, ` `, `"C:\file with a space.csv"`},
		},
		{
			name: "testSingleQuote",
			args: args{query: `SELECT * FROM 'C:\file with a space.csv'`},
			want: []string{`SELECT`, ` `, `*`, ` `, `FROM`, ` `, `'C:\file with a space.csv'`},
		},
		{
			name: "testSingleQuote2",
			args: args{query: "SELECT * FROM jame's.csv"},
			want: []string{"SELECT", " ", "*", " ", "FROM", " ", "jame's.csv"},
		},
		{
			name: "testBackQuote",
			args: args{query: "SELECT * FROM `C:\file with a space.csv`;"},
			want: []string{`SELECT`, ` `, `*`, ` `, `FROM`, ` `, "`C:\file with a space.csv`", `;`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SQLFields(tt.args.query); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SQLFields() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func newDBTestSqlite3() *DB {
	db, err := Connect("sqlite3", "")
	if err != nil {
		return nil
	}
	return db
}

func newDBTestPostgres() *DB {
	db, err := Connect("postgres", "dbname=trdsql_test")
	if err != nil {
		return nil
	}
	err = db.Ping()
	if err != nil {
		return nil
	}
	return db
}

func newDBTestMysql() *DB {
	db, err := Connect("mysql", "root@/trdsql_test")
	if err != nil {
		return nil
	}
	err = db.Ping()
	if err != nil {
		return nil
	}
	return db
}

func csvReadOpts() *ReadOpts {
	opts := NewReadOpts()
	opts.InFormat = CSV
	return opts
}

func TestImportFile(t *testing.T) {
	type args struct {
		db       *DB
		fileName string
		opts     *ReadOpts
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "testNoFile",
			args: args{
				db:       newDBTestSqlite3(),
				fileName: "nofile",
				opts:     NewReadOpts(),
			},
			want:    "",
			wantErr: false,
		},
		{
			name: "testGlobFile",
			args: args{
				db:       newDBTestSqlite3(),
				fileName: "testdata/test*.csv",
				opts:     NewReadOpts(),
			},
			want:    "`testdata/test*.csv`",
			wantErr: false,
		},
		{
			name: "testNoMatch",
			args: args{
				db:       newDBTestSqlite3(),
				fileName: "testdata/testtttttt*.csv",
				opts:     NewReadOpts(),
			},
			want:    "",
			wantErr: false,
		},
		{
			name: "testCSV",
			args: args{
				db:       newDBTestSqlite3(),
				fileName: "testdata/test.csv",
				opts:     csvReadOpts(),
			},
			want:    "`testdata/test.csv`",
			wantErr: false,
		},
		{
			name: "testSqlite",
			args: args{
				db:       newDBTestSqlite3(),
				fileName: "testdata/test.csv",
				opts:     NewReadOpts(),
			},
			want:    "`testdata/test.csv`",
			wantErr: false,
		},
		{
			name: "testPostgres",
			args: args{
				db:       newDBTestPostgres(),
				fileName: "testdata/test.csv",
				opts:     NewReadOpts(),
			},
			want:    "\"testdata/test.csv\"",
			wantErr: false,
		},
		{
			name: "testMysql",
			args: args{
				db:       newDBTestMysql(),
				fileName: "testdata/test.csv",
				opts:     NewReadOpts(),
			},
			want:    "`testdata/test.csv`",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.db == nil {
				t.SkipNow()
			}
			var err error
			tt.args.db.Tx, err = tt.args.db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			got, err := ImportFile(tt.args.db, tt.args.fileName, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ImportFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ImportFile() = %v, want %v", got, tt.want)
			}
			err = tt.args.db.Tx.Commit()
			if err != nil {
				t.Fatal(err)
			}
			err = tt.args.db.Disconnect()
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func Test_guessExtension(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		want      Format
	}{
		{name: "testCSV", tableName: "test.csv", want: CSV},
		{name: "testLTSV", tableName: "test.ltsv", want: LTSV},
		{name: "testLTSV2", tableName: "test.ltsv.gz", want: LTSV},
		{name: "testJSON", tableName: "test.json", want: JSON},
		{name: "testTBLN", tableName: "test.tbln", want: TBLN},
		{name: "testunknown", tableName: "test.go", want: CSV},
		{name: "testunknown2", tableName: "testltsv", want: CSV},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := guessExtension(tt.tableName); got != tt.want {
				t.Errorf("guessExtension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_trimQuote(t *testing.T) {
	type args struct {
		fileName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test1",
			args: args{"test"},
			want: "test",
		},
		{
			name: "test2",
			args: args{"`test`"},
			want: "test",
		},
		{
			name: "test3",
			args: args{"\"test\""},
			want: "test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := trimQuote(tt.args.fileName); got != tt.want {
				t.Errorf("trimQuote() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_extFileReader(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		want     []byte
	}{
		{
			name:     "testGzFile",
			fileName: filepath.Join("testdata", "test.csv.gz"),
			want:     []byte("1,Orange\n2,Melon\n3,Apple\n"),
		},
		{
			name:     "testNoGzFile",
			fileName: filepath.Join("testdata", "testNoGzFile.gz"),
			want:     []byte("1,Orange\n2,Melon\n3,Apple\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := os.Open(tt.fileName)
			if err != nil {
				t.Fatalf("extFileReader() file open error %s:%s", tt.fileName, err)
			}
			got := extFileReader(tt.fileName, file)
			r, err := ioutil.ReadAll(got)
			if err != nil {
				t.Fatalf("extFileReader() read error %s:%s", tt.fileName, err)
			}
			if !reflect.DeepEqual(r, tt.want) {
				t.Errorf("extFileReader() = %v, want %v", string(r), string(tt.want))
			}
		})
	}
}
