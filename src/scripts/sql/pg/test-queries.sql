-- Test-only queries for RequestQueueTest (local scratch database only).
-- {{schema}} is the test's queue schema, validated by PostgresQueueStore.IDENTIFIER.

-- Query: count_jobs
SELECT COUNT(*)::int AS count FROM {{schema}}.request_queue;

-- Query: count_jobs_with_status
SELECT COUNT(*)::int AS count FROM {{schema}}.request_queue WHERE status = $1;

-- Query: count_jobs_containing
SELECT COUNT(*)::int AS count FROM {{schema}}.request_queue WHERE request::text LIKE '%' || $1 || '%';
