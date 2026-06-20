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
