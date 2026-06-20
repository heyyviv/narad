CREATE TABLE IF NOT EXISTS log_dims (
  log_id      UUID  NOT NULL,
  log_ts      TIMESTAMPTZ NOT NULL,
  key         TEXT  NOT NULL,
  value       TEXT  NOT NULL,
  confidence  REAL  NOT NULL DEFAULT 1.0,
  PRIMARY KEY (log_id, log_ts, key)
);

SELECT create_hypertable('log_dims', by_range('log_ts'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_log_dims_kv    ON log_dims (key, value, log_ts DESC);
CREATE INDEX IF NOT EXISTS idx_log_dims_value ON log_dims (value, log_ts DESC);
