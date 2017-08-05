package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

type database struct {
	Driver string `json:"driver"`
	Dsn    string `json:"dsn"`
}

type config struct {
	Db       string              `json:"db"`
	Database map[string]database `json:"database"`
}

func configOpen(config string) io.Reader {
	fname := ""
	if config != "" {
		fname = config
	} else if runtime.GOOS == "windows" {
		fname = filepath.Join(os.Getenv("APPDATA"), "trdsql", "config.json")
	} else {
		fname = filepath.Join(os.Getenv("HOME"), ".config", "trdsql", "config.json")
	}
	cfg, err := os.Open(fname)
	if err != nil {
		debug.Printf("configOpen: %s", err.Error())
		return nil
	}
	debug.Printf("config found: %s", fname)
	return cfg
}

func loadConfig(conf io.Reader) (*config, error) {
	var cfg config
	if conf == nil {
		return &cfg, errors.New("no file")
	}
	err := json.NewDecoder(conf).Decode(&cfg)
	if err != nil {
		return &cfg, errors.New("config error")
	}
	return &cfg, nil
}
