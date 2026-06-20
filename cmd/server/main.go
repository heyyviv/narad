package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/narad/narad/internal/api"
	"github.com/narad/narad/internal/config"
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

	// Using context.Background() for migrations because the startup ctx timeout might be too short if DB is slow to start
	// Alternatively, just increase timeout. 10s might be okay.
	if err := store.RunMigrations(context.Background(), "migrations"); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	router := api.NewRouter(store)

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	go func() {
		fmt.Printf("Narad server listening on port %s\n", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("Shutting down server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	fmt.Println("Server exiting")
}
