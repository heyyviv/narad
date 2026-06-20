package config

import "os"

type Config struct {
	DatabaseURL string
	RedisURL    string
	Port        string
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
	return &Config{
		DatabaseURL: dbURL,
		RedisURL:    redisURL,
		Port:        port,
	}
}
