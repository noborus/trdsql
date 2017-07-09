package main

import (
	"encoding/json"
	"log"
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

func loadConfig() (*config, error) {
	home := os.Getenv("HOME")
	if home == "" && runtime.GOOS == "windows" {
		home = os.Getenv("APPDATA")
	}
	fname := filepath.Join(home, ".config", "csvq", "config.json")
	log.Println(fname)
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var cfg config
	err = json.NewDecoder(f).Decode(&cfg)
	return &cfg, err
}
