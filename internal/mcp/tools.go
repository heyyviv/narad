package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/narad/narad/internal/storage"
	"github.com/mark3labs/mcp-go/mcp"
)

type ToolHandlers struct {
	store *storage.Storage
}

func NewToolHandlers(store *storage.Storage) *ToolHandlers {
	return &ToolHandlers{store: store}
}

type ToolLogResult struct {
	Timestamp string            `json:"timestamp"`
	Service   string            `json:"service"`
	Level     string            `json:"level"`
	Message   string            `json:"message"`
	Code      string            `json:"code,omitempty"`
	Dims      map[string]string `json:"dims,omitempty"`
	Meta      map[string]any    `json:"meta,omitempty"`
}

// Helper to construct structured log results
func mapToToolLogResult(logs []*storage.LogEvent) []ToolLogResult {
	results := make([]ToolLogResult, len(logs))
	for i, log := range logs {
		codeStr := ""
		if log.Code != nil {
			codeStr = *log.Code
		}
		results[i] = ToolLogResult{
			Timestamp: log.Ts.Format(time.RFC3339Nano),
			Service:   log.Service,
			Level:     log.Level,
			Message:   log.Msg,
			Code:      codeStr,
			Dims:      log.Dims,
			Meta:      log.Meta,
		}
	}
	return results
}

// Helper to fetch dimensions for a batch of logs
func (h *ToolHandlers) fetchDimsForLogs(ctx context.Context, logs []*storage.LogEvent) error {
	if len(logs) == 0 {
		return nil
	}
	logIDs := make([]string, len(logs))
	logMap := make(map[uuid.UUID]*storage.LogEvent)
	for i, log := range logs {
		logIDs[i] = fmt.Sprintf("'%s'", log.ID.String())
		logMap[log.ID] = log
	}

	dimQuery := fmt.Sprintf("SELECT log_id, key, value FROM log_dims WHERE log_id IN (%s)", strings.Join(logIDs, ","))
	rows, err := h.store.Pool().Query(ctx, dimQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var logID uuid.UUID
		var k, v string
		if err := rows.Scan(&logID, &k, &v); err == nil {
			if log, ok := logMap[logID]; ok {
				if log.Dims == nil {
					log.Dims = make(map[string]string)
				}
				log.Dims[k] = v
			}
		}
	}
	return nil
}

// 1. search_logs
type SearchLogsArgs struct {
	Query   string            `json:"query"`
	Service string            `json:"service"`
	Level   string            `json:"level"`
	From    string            `json:"from"`
	To      string            `json:"to"`
	Limit   int               `json:"limit"`
	Dims    map[string]string `json:"dims"`
}

func (h *ToolHandlers) SearchLogs(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args SearchLogsArgs
	if err := request.BindArguments(&args); err != nil {
		return mcp.NewToolResultErrorFromErr("failed to parse arguments", err), nil
	}

	var fromTime, toTime time.Time
	if args.From != "" {
		t, err := time.Parse(time.RFC3339, args.From)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid from timestamp: %v", err)), nil
		}
		fromTime = t
	}
	if args.To != "" {
		t, err := time.Parse(time.RFC3339, args.To)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid to timestamp: %v", err)), nil
		}
		toTime = t
	}

	limit := args.Limit
	if limit <= 0 {
		limit = 100
	} else if limit > 1000 {
		limit = 1000
	}

	queryStr := "SELECT l.id, l.ts, l.received_at, l.svc_ts, l.service, l.level, l.code, l.msg, l.tier, l.confidence, l.meta, l.raw FROM logs l "
	var qargs []interface{}
	var joins []string
	var wheres []string
	argID := 1

	for k, v := range args.Dims {
		joinAlias := fmt.Sprintf("d%d", argID)
		joins = append(joins, fmt.Sprintf("JOIN log_dims %s ON l.id = %s.log_id AND l.ts = %s.log_ts", joinAlias, joinAlias, joinAlias))
		wheres = append(wheres, fmt.Sprintf("%s.key = $%d", joinAlias, argID))
		qargs = append(qargs, k)
		argID++
		wheres = append(wheres, fmt.Sprintf("%s.value = $%d", joinAlias, argID))
		qargs = append(qargs, v)
		argID++
	}

	if args.Query != "" {
		wheres = append(wheres, fmt.Sprintf("l.msg ILIKE $%d", argID))
		qargs = append(qargs, "%"+args.Query+"%")
		argID++
	}
	if args.Service != "" {
		wheres = append(wheres, fmt.Sprintf("l.service = $%d", argID))
		qargs = append(qargs, args.Service)
		argID++
	}
	if args.Level != "" {
		wheres = append(wheres, fmt.Sprintf("l.level = $%d", argID))
		qargs = append(qargs, args.Level)
		argID++
	}
	if !fromTime.IsZero() {
		wheres = append(wheres, fmt.Sprintf("l.ts >= $%d", argID))
		qargs = append(qargs, fromTime)
		argID++
	}
	if !toTime.IsZero() {
		wheres = append(wheres, fmt.Sprintf("l.ts <= $%d", argID))
		qargs = append(qargs, toTime)
		argID++
	}

	if len(joins) > 0 {
		queryStr += strings.Join(joins, " ") + " "
	}
	if len(wheres) > 0 {
		queryStr += "WHERE " + strings.Join(wheres, " AND ") + " "
	}

	queryStr += "ORDER BY l.ts DESC "
	queryStr += fmt.Sprintf("LIMIT $%d", argID)
	qargs = append(qargs, limit)

	rows, err := h.store.Pool().Query(ctx, queryStr, qargs...)
	if err != nil {
		return mcp.NewToolResultErrorFromErr("query failed", err), nil
	}
	defer rows.Close()

	var logs []*storage.LogEvent
	for rows.Next() {
		var log storage.LogEvent
		err := rows.Scan(
			&log.ID, &log.Ts, &log.ReceivedAt, &log.SvcTs, &log.Service,
			&log.Level, &log.Code, &log.Msg, &log.Tier, &log.Confidence,
			&log.Meta, &log.Raw,
		)
		if err != nil {
			return mcp.NewToolResultErrorFromErr("scan row failed", err), nil
		}
		logs = append(logs, &log)
	}

	if err := h.fetchDimsForLogs(ctx, logs); err != nil {
		return mcp.NewToolResultErrorFromErr("fetching dimensions failed", err), nil
	}

	results := mapToToolLogResult(logs)
	resBytes, _ := json.MarshalIndent(results, "", "  ")
	return mcp.NewToolResultText(string(resBytes)), nil
}

// 2. trace_request
type TraceRequestArgs struct {
	TraceID  string `json:"trace_id"`
	TraceKey string `json:"trace_key"`
}

func (h *ToolHandlers) TraceRequest(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args TraceRequestArgs
	if err := request.BindArguments(&args); err != nil {
		return mcp.NewToolResultErrorFromErr("failed to parse arguments", err), nil
	}

	if args.TraceID == "" {
		return mcp.NewToolResultError("trace_id is required"), nil
	}

	var queryStr string
	var qargs []interface{}
	qargs = append(qargs, args.TraceID)

	if args.TraceKey != "" {
		queryStr = `
			SELECT DISTINCT l.id, l.ts, l.received_at, l.svc_ts, l.service, l.level, l.code, l.msg, l.tier, l.confidence, l.meta, l.raw
			FROM logs l
			JOIN log_dims d ON l.id = d.log_id AND l.ts = d.log_ts
			WHERE d.value = $1 AND d.key = $2
			ORDER BY l.ts ASC
		`
		qargs = append(qargs, args.TraceKey)
	} else {
		queryStr = `
			SELECT DISTINCT l.id, l.ts, l.received_at, l.svc_ts, l.service, l.level, l.code, l.msg, l.tier, l.confidence, l.meta, l.raw
			FROM logs l
			JOIN log_dims d ON l.id = d.log_id AND l.ts = d.log_ts
			WHERE d.value = $1 AND d.key IN ('trace_id', 'request_id', 'traceId', 'requestId', 'correlation_id', 'order_id', 'customer_id', 'txn_id')
			ORDER BY l.ts ASC
		`
	}

	rows, err := h.store.Pool().Query(ctx, queryStr, qargs...)
	if err != nil {
		return mcp.NewToolResultErrorFromErr("query failed", err), nil
	}
	defer rows.Close()

	var logs []*storage.LogEvent
	for rows.Next() {
		var log storage.LogEvent
		err := rows.Scan(
			&log.ID, &log.Ts, &log.ReceivedAt, &log.SvcTs, &log.Service,
			&log.Level, &log.Code, &log.Msg, &log.Tier, &log.Confidence,
			&log.Meta, &log.Raw,
		)
		if err != nil {
			return mcp.NewToolResultErrorFromErr("scan row failed", err), nil
		}
		logs = append(logs, &log)
	}

	// If no logs found without key, search all log_dims value as fallback
	if len(logs) == 0 && args.TraceKey == "" {
		fallbackQuery := `
			SELECT DISTINCT l.id, l.ts, l.received_at, l.svc_ts, l.service, l.level, l.code, l.msg, l.tier, l.confidence, l.meta, l.raw
			FROM logs l
			JOIN log_dims d ON l.id = d.log_id AND l.ts = d.log_ts
			WHERE d.value = $1
			ORDER BY l.ts ASC
		`
		fallbackRows, err := h.store.Pool().Query(ctx, fallbackQuery, args.TraceID)
		if err == nil {
			defer fallbackRows.Close()
			for fallbackRows.Next() {
				var log storage.LogEvent
				err := fallbackRows.Scan(
					&log.ID, &log.Ts, &log.ReceivedAt, &log.SvcTs, &log.Service,
					&log.Level, &log.Code, &log.Msg, &log.Tier, &log.Confidence,
					&log.Meta, &log.Raw,
				)
				if err == nil {
					logs = append(logs, &log)
				}
			}
		}
	}

	if err := h.fetchDimsForLogs(ctx, logs); err != nil {
		return mcp.NewToolResultErrorFromErr("fetching dimensions failed", err), nil
	}

	results := mapToToolLogResult(logs)
	resBytes, _ := json.MarshalIndent(results, "", "  ")
	return mcp.NewToolResultText(string(resBytes)), nil
}

// 3. get_errors
type GetErrorsArgs struct {
	Service         string `json:"service"`
	LookbackMinutes int    `json:"lookback_minutes"`
	Limit           int    `json:"limit"`
}

type ErrorGroup struct {
	Pattern       string    `json:"pattern"`
	Service       string    `json:"service"`
	Level         string    `json:"level"`
	Code          string    `json:"code,omitempty"`
	Count         int       `json:"count"`
	LastSeen      time.Time `json:"last_seen"`
	SampleMessage string    `json:"sample_message"`
}

var (
	uuidRegex  = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	hexRegex   = regexp.MustCompile(`0x[0-9a-fA-F]+`)
	numRegex   = regexp.MustCompile(`\b\d+\b`)
	emailRegex = regexp.MustCompile(`[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}`)
	idRegex    = regexp.MustCompile(`\b(cust|txn|ord|usr|user|job|task)_[a-zA-Z0-9]+\b`)
)

func getMessagePattern(msg string) string {
	pattern := uuidRegex.ReplaceAllString(msg, "{uuid}")
	pattern = hexRegex.ReplaceAllString(pattern, "{hex}")
	pattern = idRegex.ReplaceAllString(pattern, "{id}")
	pattern = emailRegex.ReplaceAllString(pattern, "{email}")
	pattern = numRegex.ReplaceAllString(pattern, "{num}")
	return strings.TrimSpace(pattern)
}

func (h *ToolHandlers) GetErrors(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args GetErrorsArgs
	if err := request.BindArguments(&args); err != nil {
		return mcp.NewToolResultErrorFromErr("failed to parse arguments", err), nil
	}

	lookback := args.LookbackMinutes
	if lookback <= 0 {
		lookback = 60
	}
	cutoff := time.Now().Add(-time.Duration(lookback) * time.Minute)

	limit := args.Limit
	if limit <= 0 {
		limit = 20
	} else if limit > 100 {
		limit = 100
	}

	queryStr := `
		SELECT l.id, l.ts, l.received_at, l.svc_ts, l.service, l.level, l.code, l.msg, l.tier, l.confidence, l.meta, l.raw
		FROM logs l
		WHERE l.level = 'ERROR' AND l.ts >= $1
	`
	var qargs []interface{}
	qargs = append(qargs, cutoff)

	if args.Service != "" {
		queryStr += " AND l.service = $2"
		qargs = append(qargs, args.Service)
	}

	queryStr += " ORDER BY l.ts DESC LIMIT 2000"

	rows, err := h.store.Pool().Query(ctx, queryStr, qargs...)
	if err != nil {
		return mcp.NewToolResultErrorFromErr("query failed", err), nil
	}
	defer rows.Close()

	groups := make(map[string]*ErrorGroup)

	for rows.Next() {
		var log storage.LogEvent
		err := rows.Scan(
			&log.ID, &log.Ts, &log.ReceivedAt, &log.SvcTs, &log.Service,
			&log.Level, &log.Code, &log.Msg, &log.Tier, &log.Confidence,
			&log.Meta, &log.Raw,
		)
		if err != nil {
			return mcp.NewToolResultErrorFromErr("scan row failed", err), nil
		}

		pattern := getMessagePattern(log.Msg)
		codeStr := ""
		if log.Code != nil {
			codeStr = *log.Code
		}

		key := fmt.Sprintf("%s:%s:%s", log.Service, codeStr, pattern)
		if g, exists := groups[key]; exists {
			g.Count++
			if log.Ts.After(g.LastSeen) {
				g.LastSeen = log.Ts
				g.SampleMessage = log.Msg
			}
		} else {
			groups[key] = &ErrorGroup{
				Pattern:       pattern,
				Service:       log.Service,
				Level:         log.Level,
				Code:          codeStr,
				Count:         1,
				LastSeen:      log.Ts,
				SampleMessage: log.Msg,
			}
		}
	}

	// Sort groups by count DESC
	var sortedGroups []*ErrorGroup
	for _, g := range groups {
		sortedGroups = append(sortedGroups, g)
	}

	sort.Slice(sortedGroups, func(i, j int) bool {
		return sortedGroups[i].Count > sortedGroups[j].Count
	})

	if len(sortedGroups) > limit {
		sortedGroups = sortedGroups[:limit]
	}

	resBytes, _ := json.MarshalIndent(sortedGroups, "", "  ")
	return mcp.NewToolResultText(string(resBytes)), nil
}

// 4. explain_incident
type ExplainIncidentArgs struct {
	Timestamp       string `json:"timestamp"`
	Service         string `json:"service"`
	LookbackMinutes int    `json:"lookback_minutes"`
	Limit           int    `json:"limit"`
}

func (h *ToolHandlers) ExplainIncident(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args ExplainIncidentArgs
	if err := request.BindArguments(&args); err != nil {
		return mcp.NewToolResultErrorFromErr("failed to parse arguments", err), nil
	}

	if args.Timestamp == "" {
		return mcp.NewToolResultError("timestamp is required"), nil
	}

	targetTime, err := time.Parse(time.RFC3339, args.Timestamp)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid timestamp format: %v", err)), nil
	}

	lookback := args.LookbackMinutes
	if lookback <= 0 {
		lookback = 5
	}

	limit := args.Limit
	if limit <= 0 {
		limit = 100
	} else if limit > 500 {
		limit = 500
	}

	fromTime := targetTime.Add(-time.Duration(lookback) * time.Minute)
	toTime := targetTime.Add(time.Duration(lookback) * time.Minute)

	queryStr := `
		SELECT l.id, l.ts, l.received_at, l.svc_ts, l.service, l.level, l.code, l.msg, l.tier, l.confidence, l.meta, l.raw
		FROM logs l
		WHERE l.ts >= $1 AND l.ts <= $2
	`
	var qargs []interface{}
	qargs = append(qargs, fromTime, toTime)

	if args.Service != "" {
		queryStr += " AND l.service = $3"
		qargs = append(qargs, args.Service)
	}

	queryStr += " ORDER BY l.ts ASC LIMIT $4"
	if args.Service != "" {
		qargs = append(qargs, limit)
	} else {
		// Limit is next param id
		queryStr = strings.Replace(queryStr, "LIMIT $4", fmt.Sprintf("LIMIT $%d", len(qargs)+1), 1)
		qargs = append(qargs, limit)
	}

	rows, err := h.store.Pool().Query(ctx, queryStr, qargs...)
	if err != nil {
		return mcp.NewToolResultErrorFromErr("query failed", err), nil
	}
	defer rows.Close()

	var logs []*storage.LogEvent
	for rows.Next() {
		var log storage.LogEvent
		err := rows.Scan(
			&log.ID, &log.Ts, &log.ReceivedAt, &log.SvcTs, &log.Service,
			&log.Level, &log.Code, &log.Msg, &log.Tier, &log.Confidence,
			&log.Meta, &log.Raw,
		)
		if err != nil {
			return mcp.NewToolResultErrorFromErr("scan row failed", err), nil
		}
		logs = append(logs, &log)
	}

	if err := h.fetchDimsForLogs(ctx, logs); err != nil {
		return mcp.NewToolResultErrorFromErr("fetching dimensions failed", err), nil
	}

	results := mapToToolLogResult(logs)
	resBytes, _ := json.MarshalIndent(results, "", "  ")
	return mcp.NewToolResultText(string(resBytes)), nil
}

// 5. tail_service
type TailServiceArgs struct {
	Service string `json:"service"`
	Limit   int    `json:"limit"`
	Level   string `json:"level"`
}

func (h *ToolHandlers) TailService(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args TailServiceArgs
	if err := request.BindArguments(&args); err != nil {
		return mcp.NewToolResultErrorFromErr("failed to parse arguments", err), nil
	}

	if args.Service == "" {
		return mcp.NewToolResultError("service is required"), nil
	}

	limit := args.Limit
	if limit <= 0 {
		limit = 50
	} else if limit > 200 {
		limit = 200
	}

	queryStr := `
		SELECT l.id, l.ts, l.received_at, l.svc_ts, l.service, l.level, l.code, l.msg, l.tier, l.confidence, l.meta, l.raw
		FROM logs l
		WHERE l.service = $1
	`
	var qargs []interface{}
	qargs = append(qargs, args.Service)

	if args.Level != "" {
		queryStr += " AND l.level = $2"
		qargs = append(qargs, args.Level)
	}

	queryStr += " ORDER BY l.ts DESC LIMIT $3"
	if args.Level != "" {
		qargs = append(qargs, limit)
	} else {
		queryStr = strings.Replace(queryStr, "LIMIT $3", fmt.Sprintf("LIMIT $%d", len(qargs)+1), 1)
		qargs = append(qargs, limit)
	}

	rows, err := h.store.Pool().Query(ctx, queryStr, qargs...)
	if err != nil {
		return mcp.NewToolResultErrorFromErr("query failed", err), nil
	}
	defer rows.Close()

	var logs []*storage.LogEvent
	for rows.Next() {
		var log storage.LogEvent
		err := rows.Scan(
			&log.ID, &log.Ts, &log.ReceivedAt, &log.SvcTs, &log.Service,
			&log.Level, &log.Code, &log.Msg, &log.Tier, &log.Confidence,
			&log.Meta, &log.Raw,
		)
		if err != nil {
			return mcp.NewToolResultErrorFromErr("scan row failed", err), nil
		}
		logs = append(logs, &log)
	}

	if err := h.fetchDimsForLogs(ctx, logs); err != nil {
		return mcp.NewToolResultErrorFromErr("fetching dimensions failed", err), nil
	}

	// Reverse array to ascending order for chronological tail log display
	for i, j := 0, len(logs)-1; i < j; i, j = i+1, j-1 {
		logs[i], logs[j] = logs[j], logs[i]
	}

	results := mapToToolLogResult(logs)
	resBytes, _ := json.MarshalIndent(results, "", "  ")
	return mcp.NewToolResultText(string(resBytes)), nil
}
