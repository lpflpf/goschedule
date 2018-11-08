package config

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

type Config struct {
	filename string
}

var configDir string

func SetConfigDir(dirname string) {
	configDir = dirname
}

func NewConfig(filename string) *Config {
	return &Config{filename: filename}
}

func (config *Config) Load(obj interface{}) {
	if data, err := ioutil.ReadFile(configDir + "/" + config.filename); err == nil {
		if err := json.Unmarshal(data, obj); err != nil {
			log.Fatal("Json decode failed, please check file " + configDir + "/" + config.filename)
		}
	} else {
		log.Fatal("Open config file " + configDir + "/" + config.filename + " failed!")
	}
}
