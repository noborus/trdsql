package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

type target struct {
	Name string `json:"name"`
	Dsn  string `json:"dsn"`
}

type config struct {
	Dbdriver string   `json:"dbdriver"`
	Target   []target `json:"target"`
}

func configOpen() (cfg io.Reader) {
	home := os.Getenv("HOME")
	if home == "" && runtime.GOOS == "windows" {
		home = os.Getenv("APPDATA")
	}
	fname := filepath.Join(home, ".config", "csvq", "config.json")
	cfg, err := os.Open(fname)
	if err != nil {
		return nil
	}
	return cfg
}

func loadConfig(cfg io.Reader) (*config, error) {
	var conf config
	if cfg == nil {
		return &conf, errors.New("no file")
	}
	err := json.NewDecoder(cfg).Decode(&conf)
	if err != nil {
		return &conf, errors.New("config error")
	}
	return &conf, nil
}
