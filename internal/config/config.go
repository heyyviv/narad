package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	DatabaseURL  string `yaml:"database_url"`
	RedisURL     string `yaml:"redis_url"`
	Port         string `yaml:"port"`
	KafkaBrokers string `yaml:"kafka_brokers"`
	KafkaTopic   string `yaml:"kafka_topic"`
	KafkaGroup   string `yaml:"kafka_group"`
}

func Load() *Config {
	// Start with default values
	cfg := &Config{}

	// Read CONFIG_PATH env, default to config.yaml in workspace root
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		return nil
	}

	// Parse YAML configuration file if it exists
	if _, err := os.Stat(configPath); err == nil {
		if data, err := os.ReadFile(configPath); err == nil {
			var fileCfg Config
			if err := yaml.Unmarshal(data, &fileCfg); err == nil {
				if fileCfg.DatabaseURL != "" {
					cfg.DatabaseURL = fileCfg.DatabaseURL
				}
				if fileCfg.RedisURL != "" {
					cfg.RedisURL = fileCfg.RedisURL
				}
				if fileCfg.Port != "" {
					cfg.Port = fileCfg.Port
				}
				if fileCfg.KafkaBrokers != "" {
					cfg.KafkaBrokers = fileCfg.KafkaBrokers
				}
				if fileCfg.KafkaTopic != "" {
					cfg.KafkaTopic = fileCfg.KafkaTopic
				}
				if fileCfg.KafkaGroup != "" {
					cfg.KafkaGroup = fileCfg.KafkaGroup
				}
			}
		}
	}

	return cfg
}
