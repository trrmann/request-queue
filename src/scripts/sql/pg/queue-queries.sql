-- Durable request queue storage (PostgresQueueStore).
-- {{schema}} is the owning application's schema, validated as a plain identifier
-- before substitution (CODING_STANDARDS 0.1 exception); all values are bound parameters.

-- Query: create_schema
CREATE SCHEMA IF NOT EXISTS {{schema}};

-- Query: create_table
CREATE TABLE IF NOT EXISTS {{schema}}.request_queue (
  job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  queue_name VARCHAR(100) NOT NULL,
  request JSONB NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'done', 'failed')),
  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMP NOT NULL DEFAULT NOW(),
  last_error TEXT,
  response JSONB,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Query: create_index
CREATE INDEX IF NOT EXISTS request_queue_due_idx
ON {{schema}}.request_queue (queue_name, status, next_attempt_at, created_at);

-- Query: insert_job
INSERT INTO {{schema}}.request_queue (queue_name, request)
VALUES ($1, $2)
RETURNING job_id, queue_name, request, status, attempts, next_attempt_at, created_at;

-- Query: claim_next
-- Oldest due job first; SKIP LOCKED lets several workers share a queue safely.
UPDATE {{schema}}.request_queue
SET status = 'processing', updated_at = NOW()
WHERE job_id = (
  SELECT job_id FROM {{schema}}.request_queue
  WHERE queue_name = $1 AND status = 'pending' AND next_attempt_at <= NOW()
  ORDER BY created_at
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING job_id, queue_name, request, status, attempts, next_attempt_at, created_at;

-- Query: reschedule_job
UPDATE {{schema}}.request_queue
SET status = 'pending', attempts = attempts + 1, next_attempt_at = $2, last_error = $3, updated_at = NOW()
WHERE job_id = $1;

-- Query: complete_job
UPDATE {{schema}}.request_queue
SET status = 'done', response = $2, updated_at = NOW()
WHERE job_id = $1;

-- Query: fail_job
UPDATE {{schema}}.request_queue
SET status = 'failed', last_error = $2, updated_at = NOW()
WHERE job_id = $1;

-- Query: delete_job
DELETE FROM {{schema}}.request_queue WHERE job_id = $1;

-- Query: release_processing
-- Crash recovery: jobs claimed by a worker that stopped go back to pending.
UPDATE {{schema}}.request_queue
SET status = 'pending', updated_at = NOW()
WHERE queue_name = $1 AND status = 'processing';

-- Query: get_job
SELECT job_id, queue_name, request, status, attempts, next_attempt_at, last_error, response, created_at
FROM {{schema}}.request_queue
WHERE job_id = $1;

-- Query: drop_schema
DROP SCHEMA IF EXISTS {{schema}} CASCADE;
