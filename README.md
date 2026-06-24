# Narad (Log Intelligence Platform)

Narad is a self-hosted log intelligence platform built for modern microservices. It acts as an intelligent log backbone that ingests structured logs with business dimensions via Redis Streams and makes them queryable by the exact keys that matter (e.g., `customer_id`, `txn_id`, `order_id`).

## Architecture

Narad utilizes a Queue-First architecture for resilience and scalability:
1. **Redis Streams**: All services push their logs directly to a Redis Stream (`logiq:logs`). This provides backpressure handling and ensures zero log loss during downtime.
2. **Narad Worker**: A Go consumer reads batches of logs from the stream and efficiently inserts them into the database.
3. **TimescaleDB**: High-performance PostgreSQL extension used for time-series data. The logs are stored in a hypertable with automatic chunking and compression policies.
4. **Narad Server**: A fast Go HTTP API used to query and trace logs across services.

## Getting Started

### Prerequisites
- Docker and Docker Compose

### Running the Platform
Simply bring up the entire stack using Docker Compose:

```bash
docker-compose up -d --build
```

This will spin up:
- TimescaleDB (PostgreSQL 16) on port `5432`
- Redis 7 on port `6379`
- `logiq-server` API on port `8080`
- 2 instances of `logiq-worker` processing the queue

### Ingesting Logs
Push JSON-formatted logs directly to the Redis stream:

```bash
docker-compose exec redis redis-cli XADD logiq:logs "*" payload '{"service": "payment-svc", "level": "ERROR", "code": "PAYMENT_DECLINED", "msg": "Card authorization failed at gateway", "dims": {"customer_id": "cust_821", "txn_id": "txn_992abc", "order_id": "ord_443"}}'
```

### Querying Logs
You can query the `logiq-server` API on port 8080 to filter by any of the dimensions you ingested.

**Query by a specific dimension:**
```bash
curl "http://localhost:8080/v1/logs?dim.customer_id=cust_821"
```

**Get the full cross-service trace for an entity:**
```bash
curl "http://localhost:8080/v1/trace/order_id/ord_443"
```

## API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/logs` | Query logs by dimensions and time |
| `GET` | `/v1/trace/:key/:value` | Full trace across services by dimension |
| `GET` | `/v1/dims/keys` | List all unique dimension keys |
| `GET` | `/v1/dims/values?key=` | List all values for a specific dimension |
| `GET` | `/v1/health` | Health check endpoint |

## MCP Server

Narad includes a Model Context Protocol (MCP) server that exposes diagnostic tools directly to AI assistants like Claude.

### Available Tools

1. **`search_logs`**: Search structured logs by query string, service, level, timestamps, and dimension filters.
2. **`trace_request`**: Correlate and trace a specific request/trace ID across all microservices.
3. **`get_errors`**: Retrieve recent error logs clustered by structural message patterns (with frequency counts).
4. **`explain_incident`**: Examine the log timeline (±5 minutes) surrounding a specific incident timestamp.
5. **`tail_service`**: Fetch chronological, real-time tail of logs for a service.

### Connecting Claude Code to Narad MCP

To connect **Claude Code** to the Narad MCP server, add the following to your `.claude/settings.json` (or global `~/.claude/settings.json`):

#### Option A: Local Stdio Connection (Recommended for Local Dev)
Run the MCP binary directly from your host, pointing to your local TimescaleDB instance:

```json
{
  "mcpServers": {
    "narad-mcp": {
      "command": "go",
      "args": ["run", "./cmd/mcp"],
      "env": {
        "DATABASE_URL": "postgres://logiq:logiq@localhost:5432/logiq?sslmode=disable"
      }
    }
  }
}
```

#### Option B: Docker Connection (Connecting to the Compose Network)
Run the containerized MCP server in stdio mode:

```json
{
  "mcpServers": {
    "narad-mcp": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "--network",
        "narad_default",
        "-e",
        "DATABASE_URL=postgres://logiq:logiq@db:5432/logiq?sslmode=disable",
        "narad-mcp:latest"
      ]
    }
  }
}
```

#### Option C: Remote SSE Connection (Over HTTP with API Key Authentication)
Connect to the SSE server running in Docker Compose (port `8090`). Enforces header authentication:

```json
{
  "mcpServers": {
    "narad-mcp-sse": {
      "url": "http://localhost:8090/sse",
      "headers": {
        "X-API-Key": "narad_mcp_api_key_secret"
      }
    }
  }
}
```

## Kafka Ingestion

Narad supports high-throughput ingestion from **Kafka** topics alongside Redis Streams. 

To ingest logs via Kafka:
1. Ensure the `KAFKA_BROKERS` environment variable is set (e.g., `kafka:29092` or `localhost:9092`).
2. Produce structured JSON logs to the `logiq-logs` topic. The Narad Worker will automatically consume them in batches, ingest them into TimescaleDB, and commit offsets.
