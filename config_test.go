package main

import (
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	const confStream = `
	{"dbdriver": "sqlite3"}
	`
	cfg, err := loadConfig(strings.NewReader(confStream))
	if err != nil {
		t.Fatal(err.Error())
	}
	if cfg.Dbdriver != "sqlite3" {
		t.Fatalf("cfg.DbDriver set invalid")
	}
}
