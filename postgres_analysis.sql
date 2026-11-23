-- Homework 3, Part 2.2: where does PostgreSQL spend its time?
-- Run in pgAdmin or psql AFTER a benchmark run.

-- One-time setup (needs a server restart after the first line):
--   ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
--   ALTER SYSTEM SET log_lock_waits = on;        -- logs every lock wait longer than deadlock_timeout
--   -- restart PostgreSQL (Windows: Services -> postgresql-x64-16 -> Restart)
--   CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Before each run: start counting from zero
SELECT pg_stat_statements_reset();

-- 1) Server execution time per statement
SELECT left(regexp_replace(query, '\s+', ' ', 'g'), 60)                      AS query,
       calls,
       round(total_exec_time::numeric)                                        AS total_ms,
       round(mean_exec_time::numeric, 3)                                      AS mean_ms,
       round((100 * total_exec_time / sum(total_exec_time) OVER ())::numeric, 1) AS pct
FROM pg_stat_statements
WHERE calls > 50
ORDER BY total_exec_time DESC;

-- 2) Execution plan of the full scan
EXPLAIN (ANALYZE, BUFFERS) SELECT sum(data_value) FROM blocking_data;

-- 3) Table growth: inserts and old row versions make every full scan read more pages
SELECT pg_relation_size('blocking_data') / 8192 AS pages,
       n_dead_tup,
       n_tup_upd,
       n_tup_hot_upd                            -- HOT = update on the same page, no index change
FROM pg_stat_user_tables
WHERE relname = 'blocking_data';

-- 4) Lock waits right now (run WHILE a benchmark is running)
SELECT pid, pg_blocking_pids(pid) AS blocked_by, wait_event_type, wait_event,
       left(query, 50) AS query
FROM pg_stat_activity
WHERE datname = current_database() AND state <> 'idle';
