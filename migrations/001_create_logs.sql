CREATE EXTENSION IF NOT EXISTS timescaledb;

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

SELECT create_hypertable('logs', by_range('ts'), if_not_exists => TRUE);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_logs_service   ON logs (service, ts DESC);
CREATE INDEX IF NOT EXISTS idx_logs_level     ON logs (level, ts DESC);
CREATE INDEX IF NOT EXISTS idx_logs_code      ON logs (code) WHERE code IS NOT NULL;
