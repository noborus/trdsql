package main

import (
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	const confStream = `
	{"db": "sqlite3",
	 "database": [
	   { "name": "sample",
			 "dbdriver": "sqlite3",
	     "dns": ":memory:"}
	  ]
	}
	`
	cfg, err := loadConfig(strings.NewReader(confStream))
	if err != nil {
		t.Fatal(err.Error())
	}
	if cfg.Db != "sqlite3" {
		t.Fatalf("cfg.db invalid")
	}
}
