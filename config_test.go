package trdsql

import (
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	const confStream = `
	{"db": "sqlite3",
	 "database": {
	   	"sample": {
				"driver": "sqlite3",
				"dns": ":memory:"
			}
		}
	}
	`
	cfg, err := loadConfig(strings.NewReader(confStream))
	if err != nil {
		t.Fatal(err.Error())
	}
	if cfg.Database["sample"].Driver != "sqlite3" {
		t.Fatalf("cfg.db invalid")
	}
}
