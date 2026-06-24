package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/narad/narad/internal/config"
	"github.com/narad/narad/internal/consumer"
	"github.com/narad/narad/internal/storage"
)

func main() {
	cfg := config.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.NewStorage(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	if err := store.RunMigrations(context.Background(), "migrations"); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	if cfg.RedisURL != "" {
		redisConsumer, err := consumer.NewRedisConsumer(cfg.RedisURL, store)
		if err != nil {
			log.Fatalf("Failed to initialize redis consumer: %v", err)
		}
		go redisConsumer.Start(runCtx)
	}

	if cfg.KafkaBrokers != "" {
		kafkaConsumer := consumer.NewKafkaConsumer(cfg, store)
		go kafkaConsumer.Start(runCtx)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("Shutting down worker...")
	runCancel()
	time.Sleep(1 * time.Second)
	fmt.Println("Worker exiting")
}
