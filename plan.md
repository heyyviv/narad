**Here is the updated Markdown plan** incorporating **Redis Streams** as the primary ingestion mechanism:

---

# LogIQ — Log Intelligence Platform
### Project Plan & Design (Phase 1 & 2: Store & Retrieve)

---

## 1. Problem Statement

Modern microservice architectures generate enormous volumes of logs. Today's reality:

- A single incident can produce **millions of log lines** across dozens of services
- Finding the full journey of a single customer, transaction, or order requires manually correlating logs across services

**LogIQ** solves this by acting as an intelligent log backbone — it ingests structured logs with business dimensions and makes them queryable by the keys that actually matter.

---

## 2. What We Are Building (Current Scope)

A **self-hosted Docker container** (with supporting services) that teams deploy in their own Kubernetes cluster.

Core focus:

1. **Structured log ingestion** via Redis Streams
2. **Dimension-based storage** — fast lookups by business keys
3. **Powerful retrieval** — query by any combination of dimensions, time range, level, or service

> AI chat / semantic search is deferred to a later phase.

---

## 3. Core Concept: Dimensions

Every log event carries a set of key-value pairs called **dimensions** — the business identifiers that describe *who or what* the log belongs to.

### Why Dimensions?

| Traditional Logging | LogIQ with Dimensions |
|---|---|
| Full-text search over blobs | Exact key-value index lookups |
| Grep and hope | Query: `txn_id = txn_992abc` across all services |
| Manual correlation | Automatic cross-service trace by any dimension |
| Scattered context | Full journey of any entity in one query |

---

## 4. Ingestion Strategy (Queue-First)

**All logs are sent via Redis Streams** — no direct HTTP calls from application code or Airflow tasks.

### Benefits
- Better resilience (logs are durable even if LogIQ is down)
- Natural backpressure handling
- Easy batching and horizontal scaling of consumers
- At-least-once delivery with consumer groups
- Simpler service code (just push to Redis)

**Log Format** (same structured shape):

```json
{
  "ts": 1718123456789,
  "level": "ERROR",
  "code": "PAYMENT_DECLINED",
  "msg": "Card authorization failed",
  "dims": {
    "service": "payment-svc",
    "customer_id": "cust_821",
    "txn_id": "txn_992abc",
    "order_id": "ord_443"
  },
  "meta": { ... }
}
```

Services (including Airflow tasks) push logs to a Redis Stream. LogIQ consumers read, validate, and insert into TimescaleDB.

---

## 5. High-Level Architecture

```
Services / Airflow Tasks
     ↓ (push via Redis client)
Redis Streams  (logiq:logs)
     ↓ (consumer groups)
LogIQ Workers (Go)
     ↓ (batch processing)
TimescaleDB (PostgreSQL)
   ├── logs (hypertable)
   └── log_dims
```

---

## 6. Storage Design

**Database**: TimescaleDB (PostgreSQL extension) — see previous version for schema (hypertable on `logs`, `log_dims` table, compression & retention policies).

---

## 7. Retrieval API Design

Unchanged:
- `GET /v1/logs?dim.order_id=ord_443`
- `GET /v1/trace/order_id/ord_443`

---

## 8. Go Project Structure

```bash
logiq/
├── cmd/
│   ├── server/          # HTTP API server (for queries + health)
│   └── worker/          # Redis consumer worker
│
├── internal/
│   ├── api/             # Query handlers + router
│   ├── consumer/        # Redis Stream consumer logic
│   ├── ingest/          # Validation & normalization
│   ├── storage/         # DB operations (batch insert with COPY)
│   └── config/
│
├── migrations/
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## 9. Full API Surface (Phase 1-2)

| Method | Path                    | Description |
|--------|-------------------------|-----------|
| `GET`  | `/v1/logs`              | Query logs |
| `GET`  | `/v1/trace/:key/:value` | Full trace by dimension |
| `GET`  | `/v1/dims/keys`         | List dimension keys |
| `GET`  | `/v1/dims/values?key=`  | List values for key |
| `GET`  | `/v1/health`            | Health check |

> Note: Ingestion is now **queue-only** (no `/v1/log` or `/v1/log/batch` HTTP endpoints in Phase 1).

---

## 10. Deployment (docker-compose.yml)

```yaml
services:
  logiq-server:
    image: logiq:latest
    command: server
    ports:
      - "8080:8080"
    environment:
      DATABASE_URL: postgres://logiq:logiq@db:5432/logiq
      REDIS_URL: redis://redis:6379
    depends_on:
      - db
      - redis

  logiq-worker:
    image: logiq:latest
    command: worker
    environment:
      DATABASE_URL: postgres://logiq:logiq@db:5432/logiq
      REDIS_URL: redis://redis:6379
    deploy:
      replicas: 2                    # scale horizontally
    depends_on:
      - db
      - redis

  redis:
    image: redis:7-alpine
    command: redis-server --appendonly yes
    ports:
      - "6379:6379"
    volumes:
      - redisdata:/data

  db:
    image: timescale/timescaledb:latest-pg16
    environment:
      POSTGRES_USER: logiq
      POSTGRES_PASSWORD: logiq
      POSTGRES_DB: logiq
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
  redisdata:
```

---

## 11. Build Phases

### Phase 1 — Foundation (Current)
- TimescaleDB schema + hypertables + compression
- Redis Streams consumer with batch processing
- Core query + trace endpoints
- Docker + docker-compose with Redis
- Basic observability (metrics on queue depth, ingest rate, etc.)

### Phase 2 — Reliability & DX
- Official Go SDK / helper libraries (Redis producer)
- Airflow integration examples
- Dead Letter Queue (DLQ) handling
- Consumer scaling & monitoring
- Optimized batch inserts (`COPY`)

### Phase 3 — AI Chat (Deferred)
- pgvector semantic search

### Phase 4 — Advanced
- ClickHouse cold storage (optional)
- Dashboard
- Multi-tenancy

---

## 12. Key Design Decisions Summary

| Decision              | Choice                    | Reason |
|-----------------------|---------------------------|--------|
| Language              | Go                        | Performance + single binary |
| Ingestion             | **Redis Streams only**    | Resilience, decoupling, batching |
| Storage               | TimescaleDB               | Time-series performance + Postgres familiarity |
| Delivery              | Queue-first               | No log loss during downtime |
| Dim storage           | Separate `log_dims` table | Fast exact lookups |
| Deployment            | Docker + Redis + TimescaleDB | Still easy to self-host |

---

This version is clean, modern, and production-ready.

Would you like me to also generate:
- The Redis consumer code skeleton in Go?
- Producer helper examples (Python for Airflow, Go SDK)?
- Updated detailed schema file?

Let me know!