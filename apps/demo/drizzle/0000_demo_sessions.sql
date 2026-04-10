CREATE TABLE IF NOT EXISTS demo_sessions (
  id TEXT PRIMARY KEY,
  model TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('durable', 'direct')),
  sequence INTEGER NOT NULL DEFAULT 0,
  backend TEXT NOT NULL,
  text TEXT NOT NULL DEFAULT '',
  completion_tokens INTEGER NOT NULL DEFAULT 0,
  failure TEXT,
  failure_handled INTEGER NOT NULL DEFAULT 0,
  stopped INTEGER NOT NULL DEFAULT 0,
  terminal INTEGER NOT NULL DEFAULT 0,
  degraded INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS demo_entries (
  stream_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  sequence INTEGER,
  wire TEXT NOT NULL,
  terminal INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (stream_id, ordinal),
  FOREIGN KEY (stream_id) REFERENCES demo_sessions(id) ON DELETE CASCADE
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_demo_entries_stream_ordinal
ON demo_entries(stream_id, ordinal);
--> statement-breakpoint
PRAGMA optimize;
