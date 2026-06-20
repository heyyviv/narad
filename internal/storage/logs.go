package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type LogEvent struct {
	ID         uuid.UUID              `json:"id"`
	Ts         time.Time              `json:"ts"`
	ReceivedAt time.Time              `json:"received_at"`
	SvcTs      *int64                 `json:"svc_ts,omitempty"`
	Service    string                 `json:"service"`
	Level      string                 `json:"level"`
	Code       *string                `json:"code,omitempty"`
	Msg        string                 `json:"msg"`
	Dims       map[string]string      `json:"dims,omitempty"`
	Meta       map[string]interface{} `json:"meta,omitempty"`
	Tier       int                    `json:"tier"`
	Confidence float32                `json:"confidence"`
	Raw        *string                `json:"raw,omitempty"`
}

func (s *Storage) InsertLog(ctx context.Context, log *LogEvent) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if log.ID == uuid.Nil {
		log.ID = uuid.New()
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO logs (id, ts, svc_ts, service, level, code, msg, tier, confidence, meta, raw)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, log.ID, log.Ts, log.SvcTs, log.Service, log.Level, log.Code, log.Msg, log.Tier, log.Confidence, log.Meta, log.Raw)
	if err != nil {
		return fmt.Errorf("failed to insert log: %w", err)
	}

	for k, v := range log.Dims {
		_, err = tx.Exec(ctx, `
			INSERT INTO log_dims (log_id, log_ts, key, value, confidence)
			VALUES ($1, $2, $3, $4, $5)
		`, log.ID, log.Ts, k, v, log.Confidence)
		if err != nil {
			return fmt.Errorf("failed to insert dim %s: %w", k, err)
		}
	}

	return tx.Commit(ctx)
}

func (s *Storage) InsertLogBatch(ctx context.Context, logs []*LogEvent) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	b := &pgx.Batch{}

	for _, log := range logs {
		if log.ID == uuid.Nil {
			log.ID = uuid.New()
		}

		b.Queue(`
			INSERT INTO logs (id, ts, svc_ts, service, level, code, msg, tier, confidence, meta, raw)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`, log.ID, log.Ts, log.SvcTs, log.Service, log.Level, log.Code, log.Msg, log.Tier, log.Confidence, log.Meta, log.Raw)

		for k, v := range log.Dims {
			b.Queue(`
				INSERT INTO log_dims (log_id, log_ts, key, value, confidence)
				VALUES ($1, $2, $3, $4, $5)
			`, log.ID, log.Ts, k, v, log.Confidence)
		}
	}

	br := tx.SendBatch(ctx, b)

	for i := 0; i < b.Len(); i++ {
		_, err := br.Exec()
		if err != nil {
			br.Close()
			return fmt.Errorf("batch execution failed at statement %d: %w", i, err)
		}
	}

	if err := br.Close(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
