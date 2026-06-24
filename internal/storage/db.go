package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Storage struct {
	pool *pgxpool.Pool
}

func NewStorage(ctx context.Context, dbURL string) (*Storage, error) {
	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse db config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	return &Storage{pool: pool}, nil
}

func (s *Storage) RunMigrations(ctx context.Context, migrationsDir string) error {
	files, err := os.ReadDir(migrationsDir)
	if err != nil {
		fmt.Printf("Skipping migrations, dir error: %v\n", err)
		return nil
	}
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sql" {
			path := filepath.Join(migrationsDir, f.Name())
			content, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("failed to read migration %s: %w", path, err)
			}
			_, err = s.pool.Exec(ctx, string(content))
			if err != nil {
				return fmt.Errorf("failed to execute migration %s: %w", path, err)
			}
			fmt.Printf("Ran migration: %s\n", f.Name())
		}
	}
	return nil
}

func (s *Storage) Close() {
	s.pool.Close()
}

func (s *Storage) Pool() *pgxpool.Pool {
	return s.pool
}
