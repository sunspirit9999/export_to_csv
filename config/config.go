package config

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"

	"github.com/spf13/viper"
)

type Config struct {
	viper.Viper
}

func Load() (*Config, error) {

	configDir := os.Getenv("CONFIG_DIR")
	fmt.Println(configDir)
	if configDir == "" {
		path, err := os.Getwd()
		if err != nil {
			log.Fatal("Cannot find CONFIG_DIR ")
		}
		configDir = path
	}

	configFile := path.Join(configDir, "config")
	config := viper.New()

	fileName := filepath.Base(configFile)
	config.SetConfigName(fileName)

	// configDir := filepath.Dir(configFile)
	config.SetConfigType("yaml")

	config.AddConfigPath(configDir)
	config.AddConfigPath("./config/")
	config.AddConfigPath("../config/")
	err := config.ReadInConfig() // Find and read the config file
	if err != nil {
		return nil, err
	}
	return &Config{
		Viper: *config,
	}, nil
}
