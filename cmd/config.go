package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/noborus/trdsql"
)

// ErrNoFile is returned when there is no file.
var ErrNoFile = errors.New("no file")

func configOpenOld(config string) io.Reader {
	var fileName string
	switch {
	case config != "":
		fileName = config
	case runtime.GOOS == "windows":
		fileName = filepath.Join(os.Getenv("APPDATA"), trdsql.AppName, "config.json")
	default:
		fileName = filepath.Join(os.Getenv("HOME"), ".config", trdsql.AppName, "config.json")
	}
	cfg, err := os.Open(fileName)
	if err != nil {
		if Debug {
			log.Printf("configOpen: %s", err.Error())
		}
		return nil
	}
	if Debug {
		log.Printf("config found: %s", fileName)
	}
	return cfg
}

func loadConfigOld(conf io.Reader) (*DBConfig, error) {
	var cfg DBConfig
	if conf == nil {
		return &cfg, ErrNoFile
	}
	err := json.NewDecoder(conf).Decode(&cfg)
	if err != nil {
		return &cfg, fmt.Errorf("config error: %w", err)
	}
	return &cfg, nil
}
