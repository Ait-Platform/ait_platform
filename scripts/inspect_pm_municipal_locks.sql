-- Read-only; no cancellation/termination and no user SQL text or parameters.
-- Run while a migration is waiting. Once it times out, its blockers cannot be
-- reconstructed from pg_stat_activity alone; the second query shows current locks.
BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';
SELECT waiting.pid AS waiting_pid,waiting.application_name AS waiting_application,
       waiting.state,waiting.wait_event_type,waiting.wait_event,
       now()-waiting.xact_start AS waiting_transaction_age,
       blocker.pid AS blocking_pid,blocker.application_name AS blocking_application,
       blocker.state AS blocking_state,
       now()-blocker.xact_start AS blocking_transaction_age
FROM pg_stat_activity waiting
CROSS JOIN LATERAL unnest(pg_blocking_pids(waiting.pid)) AS blocked_by(pid)
JOIN pg_stat_activity blocker ON blocker.pid=blocked_by.pid
WHERE waiting.datname=current_database()
ORDER BY waiting.xact_start,waiting.pid,blocker.pid;

SELECT a.pid,a.application_name,a.state,a.wait_event_type,a.wait_event,
       now()-a.xact_start AS transaction_age,n.nspname,c.relname,l.mode,l.granted,
       pg_blocking_pids(a.pid) AS blocked_by
FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
JOIN pg_class c ON c.oid=l.relation JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE a.datname=current_database() AND n.nspname='public'
  AND c.relname IN ('bil_property','bil_meter','bil_muni_account')
ORDER BY l.granted,a.xact_start,a.pid,c.relname;

-- Confirm rollback state before any separately approved retry.
SELECT version_num FROM alembic_version ORDER BY version_num;
SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint
WHERE conrelid='public.bil_muni_account'::regclass AND contype='u';
ROLLBACK;
