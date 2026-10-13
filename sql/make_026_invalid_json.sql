-- a pg_task.json that doesn't parse, or doesn't fit the types of its keys, is logged and left unapplied: pg_conf and pg_work keep running as they are rather than exit over it again and again
SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT pid AS conf_pid FROM pg_catalog.pg_stat_activity WHERE application_name = 'pg_conf'
\gset
SELECT pid AS work_pid FROM pg_catalog.pg_stat_activity WHERE datname = current_database() AND application_name LIKE 'pg_work public task %'
\gset
ALTER SYSTEM SET pg_task.json = '[{"data":';
SELECT pg_reload_conf();
SELECT pg_sleep(2);
SELECT pg_stat_clear_snapshot();
SELECT count(*) FILTER (WHERE pid = :conf_pid) = 1 AS same_conf_after_bad_syntax, count(*) FILTER (WHERE pid = :work_pid) = 1 AS same_work_after_bad_syntax FROM pg_catalog.pg_stat_activity;
SELECT left(:'json_baseline', -2) || ',"sleep":"x"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
SELECT pg_sleep(2);
SELECT pg_stat_clear_snapshot();
SELECT count(*) FILTER (WHERE pid = :conf_pid) = 1 AS same_conf_after_bad_type, count(*) FILTER (WHERE pid = :work_pid) = 1 AS same_work_after_bad_type FROM pg_catalog.pg_stat_activity;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
