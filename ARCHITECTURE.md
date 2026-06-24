# Narad Architecture & Development Guide

Welcome to the **Narad** Log Intelligence Platform developer guide. Narad is a high-performance, self-hosted log intelligence platform built for microservices. It ingests structured logs with business dimensions via message queues (Redis Streams and Kafka) and stores them in TimescaleDB for time-series optimized dimension query and cross-service request tracing.

---

## Table of Contents
1. [Core Philosophy & Dimensions](#1-core-philosophy--dimensions)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Database & Storage Layout (TimescaleDB)](#3-database--storage-layout-timescaledb)
4. [Ingestion Pipelines](#4-ingestion-pipelines)
   - [Redis Streams Ingestion](#redis-streams-ingestion)
   - [Kafka Ingestion](#kafka-ingestion)
5. [Worker Daemon (`cmd/worker`)](#5-worker-daemon-cmdworker)
6. [HTTP API Reference (`cmd/server`)](#6-http-api-reference-cmdserver)
7. [MCP Server (`cmd/mcp`)](#7-mcp-server-cmdmcp)
   - [Supported Diagnostics Tools](#supported-diagnostics-tools)
   - [Authentication & Transport Modes](#authentication--transport-modes)
   - [IDE Integration](#ide-integration)
8. [Docker Compose & Environment Config](#8-docker-compose--environment-config)
9. [Code Navigation & Reading Guide](#9-code-navigation--reading-guide)

---

## 1. Core Philosophy & Dimensions

Traditional logging platforms aggregate massive, unstructured text blocks that must be parsed using heavy full-text search indexes. Narad uses a **Dimension-First** approach.

### What are Dimensions?
Every log event carries key-value pairs called **dimensions**. These are business-context identifiers (e.g. `customer_id`, `txn_id`, `order_id`, `user_id`, `job_id`) which describe the transaction or entity context.
Instead of hoping text regex matches your log, you query the exact dimension:

| Query Mode | Traditional Platforms | Narad (Dimensions) |
| :--- | :--- | :--- |
| **Lookup** | `grep "cust_821"` over millions of raw blobs | Query `customer_id = cust_821` |
| **Correlation** | Manual search across services | Follow request journey via `trace_id` dimension |
| **Cost / Speed** | Heavy indexing, slow text parsing | Exact PostgreSQL table index lookup |

---

## 2. High-Level Architecture

Narad is composed of three core services and two supporting datastores:

```
                  ┌──────────────────────┐
                  │ Logs / App Producers │
                  └──────────┬───────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
       [ Redis Streams ]             [ Apache Kafka ]
        (logiq:logs)                  (logiq-logs)
              ▲                             ▲
              └──────────────┬──────────────┘
                             ▼
                    ┌─────────────────┐
                    │  Narad Workers  │  (cmd/worker)
                    └────────┬────────┘
                             │ (Batch copy/insert)
                             ▼
                    ┌─────────────────┐
                    │   TimescaleDB   │  (PostgreSQL + Timescaledb)
                    └────────┬────────┘
              ┌──────────────┴──────────────┐
              ▼                             ▼
     ┌─────────────────┐           ┌─────────────────┐
     │  Narad Servers  │           │   Narad MCP     │
     │   (HTTP API)    │           │ (IDE Assistant) │
     │  (cmd/server)   │           │   (cmd/mcp)     │
     └─────────────────┘           └─────────────────┘
```

---

## 3. Database & Storage Layout (TimescaleDB)

TimescaleDB is used to handle high-throughput time-series logs. The schema consists of two hypertables:

### 1. `logs` Hypertable
Stores core log fields. Chunked automatically by time ranges on `ts`.
```sql
CREATE TABLE IF NOT EXISTS logs (
  id          UUID            NOT NULL,
  ts          TIMESTAMPTZ     NOT NULL,
  received_at TIMESTAMPTZ     NOT NULL DEFAULT now(),
  svc_ts      BIGINT,
  service     TEXT            NOT NULL,
  level       TEXT            NOT NULL,
  code        TEXT,
  msg         TEXT            NOT NULL,
  tier        SMALLINT        NOT NULL DEFAULT 1,
  confidence  REAL            NOT NULL DEFAULT 1.0,
  meta        JSONB,
  raw         TEXT,
  UNIQUE (id, ts)
);
```

### 2. `log_dims` Hypertable
Stores the custom key-value pairs (dimensions) for each log. Useful for joining on `log_id` and `log_ts`.
```sql
CREATE TABLE IF NOT EXISTS log_dims (
  log_id      UUID  NOT NULL,
  log_ts      TIMESTAMPTZ NOT NULL,
  key         TEXT  NOT NULL,
  value       TEXT  NOT NULL,
  confidence  REAL  NOT NULL DEFAULT 1.0,
  PRIMARY KEY (log_id, log_ts, key)
);
```

### Database Indexes
To speed up queries, standard indexes are configured:
- `idx_logs_service` on `logs (service, ts DESC)`
- `idx_logs_level` on `logs (level, ts DESC)`
- `idx_log_dims_kv` on `log_dims (key, value, log_ts DESC)`
- `idx_log_dims_value` on `log_dims (value, log_ts DESC)`

---

## 4. Ingestion Pipelines

Narad supports a **Queue-First Ingestion** pattern to handle backpressure and guarantee delivery.

### Payload Schema
Both queues expect structured JSON logs:
```json
{
  "ts": "2026-06-24T19:00:00Z",
  "service": "payment-service",
  "level": "ERROR",
  "code": "PAYMENT_DECLINED",
  "msg": "Card authorization failed at gateway",
  "dims": {
    "customer_id": "cust_821",
    "txn_id": "txn_992abc",
    "order_id": "ord_443",
    "trace_id": "tr_1192b0f4"
  },
  "meta": {
    "ip_address": "12.34.56.78",
    "attempts": 3
  }
}
```

### Redis Streams Ingestion
- **Stream Name**: `logiq:logs`
- **Consumer Group**: `logiq-workers`
- **Ingestion Command Example (Redis CLI)**:
  ```bash
  docker-compose exec redis redis-cli XADD logiq:logs "*" payload '{"service": "payment-svc", "level": "ERROR", "code": "PAYMENT_DECLINED", "msg": "Authorization timeout", "dims": {"customer_id": "cust_821", "txn_id": "txn_992abc"}}'
  ```

### Kafka Ingestion
- **Topic Name**: `logiq-logs`
- **Consumer Group**: `logiq-workers-group`
- **Ingestion Example (Kafka Console Producer)**:
  ```bash
  docker-compose exec -it kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic logiq-logs
  # Paste payload here:
  # {"service": "order-svc", "level": "INFO", "msg": "Created order successfully", "dims": {"order_id": "ord_443", "trace_id": "tr_1192b0f4"}}
  ```

---

## 5. Worker Daemon (`cmd/worker`)

The Worker starts two ingestion go-routines based on active configuration:

1. **`RedisConsumer`**: Polls `logiq:logs` using `XReadGroup`. Gathers logs in batches up to 100, inserts them into TimescaleDB, and acknowledges messages via `XAck` if successful.
2. **`KafkaConsumer`**: Listens to the `logiq-logs` topic using a group coordinator. Uses a time/size-bound accumulator buffer (up to 100 messages or 100ms timeout) to batch insert into TimescaleDB before calling `CommitMessages`.

---

## 6. HTTP API Reference (`cmd/server`)

The REST Server runs on port `8080` (by default) and exposes endpoints for manual querying and validation:

### `GET /v1/logs`
Query logs. You can filter by any dimension by prefixing query parameters with `dim.`.
- **Query Params**:
  - `service`: Filter by service name.
  - `level`: Filter by level (e.g. `INFO`, `ERROR`).
  - `code`: Filter by error code.
  - `from`: Unix millisecond timestamp.
  - `to`: Unix millisecond timestamp.
  - `limit`: Max logs to return (default 100, max 1000).
  - `dim.<key>`: Match logs having dimension key/value (e.g. `dim.customer_id=cust_821`).
- **Example**:
  ```bash
  curl "http://localhost:8080/v1/logs?level=ERROR&dim.customer_id=cust_821"
  ```

### `GET /v1/trace/{key}/{value}`
Traces request flow sequentially by fetching logs matching a specific dimension across all services. Reverses database order to return chronological timeline (`ts ASC`).
- **Example**:
  ```bash
  curl "http://localhost:8080/v1/trace/order_id/ord_443"
  ```

### `GET /v1/dims/keys`
List all unique dimension keys stored in the database.

### `GET /v1/dims/values?key={key}`
List up to 50 unique values stored under a given dimension key.

### `GET /v1/health`
Health check endpoint. Returns HTTP 200 `OK`.

---

## 7. MCP Server (`cmd/mcp`)

The Model Context Protocol (MCP) server enables AI development tools (like **Claude Code**) to run diagnostics directly against Narad's database.

### Supported Diagnostics Tools

| Tool Name | Parameters | Description |
| :--- | :--- | :--- |
| **`search_logs`** | `query`, `service`, `level`, `from`, `to`, `limit`, `dims` (object) | Powerful search with time range and query substring matching. |
| **`trace_request`** | `trace_id` (req), `trace_key` (opt) | Automatically joins dimensions and traces the log journey chronologically (`ts ASC`). |
| **`get_errors`** | `service`, `lookback_minutes`, `limit` | Clustered error group diagnostic. Simplifies errors by replacing IDs/UUIDs/numbers with placeholders (`{id}`, `{uuid}`, `{num}`) in Go to cluster frequency count. |
| **`explain_incident`**| `timestamp` (req), `service` (opt), `lookback_minutes` | Retrieves a $\pm5$-minute timeline context surrounding an incident event to identify root causes. |
| **`tail_service`** | `service` (req), `limit`, `level` | Chronological tail logs (reversed to ascending) from a specific service. |

### Authentication & Transport Modes
The MCP server supports two transport mechanisms:
1. **Stdio Transport** (`stdio`): Listens on stdin/stdout. Standard for local IDE assistants. Auth is implicit (secured via local OS process execution).
2. **SSE Transport** (`sse`): Exposes an HTTP Server (port `8090`). Enforces API key verification on `/sse` and `/message`. Authenticates incoming headers (`Authorization: Bearer <key>`, `X-API-Key`) and URL parameters (`api_key`, `token`).

### IDE Integration
See the settings snippet in the main [README.md](file:///Users/vivekdas/Desktop/projects/narad/README.md#connecting-claude-code-to-narad-mcp) to see how to connect Claude Code to the MCP server.

---

## 8. Docker Compose & Environment Config

To run the entire ecosystem locally:

```bash
docker-compose up -d --build
```

### Active Services:
- `db`: TimescaleDB (PostgreSQL 16) listening on port `5432`
- `redis`: Redis 7 server listening on port `6379`
- `kafka`: Apache Kafka (KRaft mode) listening on port `9092`
- `logiq-server`: REST API listening on port `8080`
- `logiq-worker`: High-throughput Redis Streams / Kafka queue daemon
- `logiq-mcp`: SSE MCP Server listening on port `8090` (secured with API Key: `narad_mcp_api_key_secret`)

---

## 9. Code Navigation & Reading Guide

If you are new to the codebase and want to read and understand every line, we recommend starting in this sequence:

### 1. Configuration & Database Foundations
- **[internal/config/config.go](file:///Users/vivekdas/Desktop/projects/narad/internal/config/config.go)**: Understand how configuration parameters are loaded from `config.yaml` with environment variable overrides.
- **[internal/storage/db.go](file:///Users/vivekdas/Desktop/projects/narad/internal/storage/db.go)**: View database initializers, migration runners, and the connection `Pool()` accessor.

### 2. Ingestion Pipelines & Daemons
- **[internal/consumer/kafka.go](file:///Users/vivekdas/Desktop/projects/narad/internal/consumer/kafka.go)**: Review how the Kafka consumer reads in batches, processes logs, batch-inserts into TimescaleDB, and commits offsets.
- **[internal/consumer/consumer.go](file:///Users/vivekdas/Desktop/projects/narad/internal/consumer/consumer.go)**: Compare with the Redis Streams consumer setup.
- **[cmd/worker/main.go](file:///Users/vivekdas/Desktop/projects/narad/cmd/worker/main.go)**: See how the worker daemon starts both consumers concurrently in goroutines.

### 3. MCP Server & Authentication Middleware
- **[cmd/mcp/main.go](file:///Users/vivekdas/Desktop/projects/narad/cmd/mcp/main.go)**: Trace how the server flags and env variables set up stdio or SSE transport. Look at the `authMiddleware` to see how it validates headers and query tokens.

### 4. Core Diagnostic Tools
- **[internal/mcp/tools.go](file:///Users/vivekdas/Desktop/projects/narad/internal/mcp/tools.go)**: Deep-dive into each of the 5 tools:
  1. `SearchLogs`: Dynamic query construction with time range bounds and dimension joins.
  2. `TraceRequest`: Follow a specific trace ID across services with automatic database fallback.
  3. `GetErrors`: Structural grouping of recent logs by replacing variables (UUIDs, IDs, digits) in Go via regular expressions.
  4. `ExplainIncident`: Log query correlation within a temporal range around a target incident.
  5. `TailService`: Retrieves tail logs sorted chronologically ascending for human-readable layout.
