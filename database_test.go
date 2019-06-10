package trdsql

import (
	"testing"
)

func TestConnect(t *testing.T) {
	db, err := Connect("sqlite3", "") // dsn set :memory:
	if err != nil {
		t.Fatal(err.Error())
	}
	err = db.Disconnect()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestErrorSelect(t *testing.T) {
	db, err := Connect("sqlite3", "") // dsn set :memory:
	if err != nil {
		t.Fatal(err.Error())
	}
	defer func() {
		err = db.Disconnect()
		if err != nil {
			t.Fatalf("Disconnect error")
		}
	}()
	db.Tx, err = db.DB.Begin()
	if err != nil {
		t.Fatalf("Begin error")
	}
	_, err = db.Select(" ")
	if err == nil {
		t.Fatalf("Select error")
	}
	err = db.Tx.Commit()
	if err != nil {
		t.Fatalf("Commit error")
	}

	db.Tx, err = db.DB.Begin()
	if err != nil {
		t.Fatalf("Begin error")

	}
	_, err = db.Select("SELEC * FROM test")
	if err == nil {
		t.Fatalf("Select error")
	}
	err = db.Tx.Commit()
	if err != nil {
		t.Fatalf("Commit error")
	}
}

func TestRewrite(t *testing.T) {
	db, err := Connect("sqlite3", "")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer func() {
		err = db.Disconnect()
		if err != nil {
			t.Fatalf("Disconnect error")
		}
	}()
	orgstr := "SELECT test.csv.* FROM test.csv"
	query := orgstr
	query = db.RewriteSQL(query, "test.csv", "`test.csv`")
	if query != "SELECT `test.csv`.* FROM `test.csv`" {
		t.Fatal("Rewrite error")
	}
	// Do not rewrite more than 2 times
	query = db.RewriteSQL(query, "test.csv", "`test.csv`")
	if query != "SELECT `test.csv`.* FROM `test.csv`" {
		t.Fatal("Rewrite error")
	}
}

func TestEscapeName(t *testing.T) {
	db, err := Connect("sqlite3", "")
	if err != nil {
		t.Fatal("EscapeName error")
	}
	defer func() {
		err = db.Disconnect()
		if err != nil {
			t.Fatalf("Disconnect error")
		}
	}()
	es := db.EscapeName("test.csv")
	if es != "`test.csv`" {
		t.Fatalf("EscapeName error %s", es)
	}
	es = db.EscapeName("`test.csv`")
	if es != "`test.csv`" {
		t.Fatalf("EscapeName error %s", es)
	}
}
