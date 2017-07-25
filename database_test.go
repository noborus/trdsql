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
