package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

type Database struct {
	Name     string `json:"name"`
	Dbdriver string `json:"dbdriver"`
	Dsn      string `json:"dsn"`
}

type config struct {
	Db       string     `json:"db"`
	Database []Database `json:"database"`
}

func configOpen() (cfg io.Reader) {
	home := os.Getenv("HOME")
	if home == "" && runtime.GOOS == "windows" {
		home = os.Getenv("APPDATA")
	}
	fname := filepath.Join(home, ".config", "trdsql", "config.json")
	cfg, err := os.Open(fname)
	if err != nil {
		return nil
	}
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
