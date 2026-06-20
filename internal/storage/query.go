package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type QueryParams struct {
	Dims          map[string]string
	Service       string
	Level         string
	Code          string
	From          time.Time
	To            time.Time
	Limit         int
	Tier          int
	MinConfidence float32
}

func (s *Storage) QueryLogs(ctx context.Context, p QueryParams) ([]*LogEvent, error) {
	query := "SELECT l.id, l.ts, l.received_at, l.svc_ts, l.service, l.level, l.code, l.msg, l.tier, l.confidence, l.meta, l.raw FROM logs l "
	var args []interface{}
	var joins []string
	var wheres []string

	argID := 1

	for k, v := range p.Dims {
		joinAlias := fmt.Sprintf("d%d", argID)
		joins = append(joins, fmt.Sprintf("JOIN log_dims %s ON l.id = %s.log_id AND l.ts = %s.log_ts", joinAlias, joinAlias, joinAlias))
		wheres = append(wheres, fmt.Sprintf("%s.key = $%d", joinAlias, argID))
		args = append(args, k)
		argID++
		wheres = append(wheres, fmt.Sprintf("%s.value = $%d", joinAlias, argID))
		args = append(args, v)
		argID++
	}

	if p.Service != "" {
		wheres = append(wheres, fmt.Sprintf("l.service = $%d", argID))
		args = append(args, p.Service)
		argID++
	}
	if p.Level != "" {
		wheres = append(wheres, fmt.Sprintf("l.level = $%d", argID))
		args = append(args, p.Level)
		argID++
	}
	if p.Code != "" {
		wheres = append(wheres, fmt.Sprintf("l.code = $%d", argID))
		args = append(args, p.Code)
		argID++
	}
	if !p.From.IsZero() {
		wheres = append(wheres, fmt.Sprintf("l.ts >= $%d", argID))
		args = append(args, p.From)
		argID++
	}
	if !p.To.IsZero() {
		wheres = append(wheres, fmt.Sprintf("l.ts <= $%d", argID))
		args = append(args, p.To)
		argID++
	}
	if p.Tier > 0 {
		wheres = append(wheres, fmt.Sprintf("l.tier = $%d", argID))
		args = append(args, p.Tier)
		argID++
	}
	if p.MinConfidence > 0 {
		wheres = append(wheres, fmt.Sprintf("l.confidence >= $%d", argID))
		args = append(args, p.MinConfidence)
		argID++
	}

	if len(joins) > 0 {
		query += strings.Join(joins, " ") + " "
	}
	if len(wheres) > 0 {
		query += "WHERE " + strings.Join(wheres, " AND ") + " "
	}

	query += "ORDER BY l.ts DESC "

	if p.Limit <= 0 || p.Limit > 1000 {
		p.Limit = 100
	}
	query += fmt.Sprintf("LIMIT $%d", argID)
	args = append(args, p.Limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	var logs []*LogEvent
	for rows.Next() {
		var log LogEvent
		err := rows.Scan(
			&log.ID, &log.Ts, &log.ReceivedAt, &log.SvcTs, &log.Service,
			&log.Level, &log.Code, &log.Msg, &log.Tier, &log.Confidence,
			&log.Meta, &log.Raw,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		logs = append(logs, &log)
	}

	if len(logs) > 0 {
		logIDs := make([]string, len(logs))
		for i, log := range logs {
			logIDs[i] = fmt.Sprintf("'%s'", log.ID.String())
		}
		
		dimQuery := fmt.Sprintf("SELECT log_id, key, value FROM log_dims WHERE log_id IN (%s)", strings.Join(logIDs, ","))
		dimRows, err := s.pool.Query(ctx, dimQuery)
		if err == nil {
			defer dimRows.Close()
			dimMap := make(map[uuid.UUID]map[string]string)
			for dimRows.Next() {
				var logID uuid.UUID
				var k, v string
				if err := dimRows.Scan(&logID, &k, &v); err == nil {
					if _, ok := dimMap[logID]; !ok {
						dimMap[logID] = make(map[string]string)
					}
					dimMap[logID][k] = v
				}
			}
			for _, log := range logs {
				if d, ok := dimMap[log.ID]; ok {
					log.Dims = d
				}
			}
		}
	}

	return logs, nil
}

func (s *Storage) ListDimKeys(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, "SELECT DISTINCT key FROM log_dims ORDER BY key")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err == nil {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (s *Storage) ListDimValues(ctx context.Context, key string) ([]string, error) {
	rows, err := s.pool.Query(ctx, "SELECT DISTINCT value FROM log_dims WHERE key = $1 LIMIT 50", key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vals []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err == nil {
			vals = append(vals, v)
		}
	}
	return vals, nil
}
