package main

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
	_, err = db.Select(" ")
	if err == nil {
		t.Fatalf("Select error")
	}
	_, err = db.Select("SELEC * FROM test")
	if err == nil {
		t.Fatalf("Select error")
	}
}

func TestRewrite(t *testing.T) {
	db, err := Connect("sqlite3", "")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Disconnect()
	orgstr := "SELECT test.csv.* FROM test.csv"
	sqlstr := orgstr
	sqlstr = db.rewrite(sqlstr, "test.csv", "`test.csv`")
	if sqlstr != "SELECT `test.csv`.* FROM `test.csv`" {
		t.Fatal("Rewrite error")
	}
	// Do not rewrite more than 2 times
	sqlstr = db.rewrite(sqlstr, "test.csv", "`test.csv`")
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
	es := db.escapetable("test.csv")
	if es != "`test.csv`" {
		t.Fatalf("Escapetable error %s", es)
	}
	es = db.escapetable("`test.csv`")
	if es != "`test.csv`" {
		t.Fatalf("Escapetable error %s", es)
	}
}
