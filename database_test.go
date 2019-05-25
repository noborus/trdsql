package trdsql

import (
	"testing"
)

func TestConnect(t *testing.T) {
	db, err := Connect("sqlite3", "") // dsn set :memory:
	if err != nil {
		t.Fatal(err.Error())
	}
	db.Disconnect()
}

func TestErrorSelect(t *testing.T) {
	db, err := Connect("sqlite3", "") // dsn set :memory:
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Disconnect()
	db.tx, err = db.DB.Begin()
	if err != nil {
		t.Fatalf("Begin error")

	}
	_, err = db.Select(" ")
	if err == nil {
		t.Fatalf("Select error")
	}
	db.tx.Commit()

	db.tx, err = db.DB.Begin()
	if err != nil {
		t.Fatalf("Begin error")

	}
	_, err = db.Select("SELEC * FROM test")
	if err == nil {
		t.Fatalf("Select error")
	}
	db.tx.Commit()
}

func TestRewrite(t *testing.T) {
	db, err := Connect("sqlite3", "")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Disconnect()
	orgstr := "SELECT test.csv.* FROM test.csv"
	sqlstr := orgstr
	sqlstr = db.RewriteSQL(sqlstr, "test.csv", "`test.csv`")
	if sqlstr != "SELECT `test.csv`.* FROM `test.csv`" {
		t.Fatal("Rewrite error")
	}
	// Do not rewrite more than 2 times
	sqlstr = db.RewriteSQL(sqlstr, "test.csv", "`test.csv`")
	if sqlstr != "SELECT `test.csv`.* FROM `test.csv`" {
		t.Fatal("Rewrite error")
	}
}

func TestEscapetable(t *testing.T) {
	db, err := Connect("sqlite3", "")
	if err != nil {
		t.Fatal("Escapetable error")
	}
	defer db.Disconnect()
	es := db.EscapeTable("test.csv")
	if es != "`test.csv`" {
		t.Fatalf("Escapetable error %s", es)
	}
	es = db.EscapeTable("`test.csv`")
	if es != "`test.csv`" {
		t.Fatalf("Escapetable error %s", es)
	}
}
