package config

import "os"

type Config struct {
	DatabaseURL  string
	RedisURL     string
	Port         string
	KafkaBrokers string
}

func Load() *Config {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://logiq:logiq@localhost:5432/logiq?sslmode=disable"
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	return &Config{
		DatabaseURL:  dbURL,
		RedisURL:     redisURL,
		Port:         port,
		KafkaBrokers: kafkaBrokers,
	}
}
