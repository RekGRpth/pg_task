DELETE FROM task WHERE "group" = 'connection_kill_recovery';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, remote, timeout) VALUES ('connection_kill_recovery', 'SELECT pg_sleep(30)', 'dbname=' || :'DBNAME', '1 min');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(30)' AND state = 'active') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for remote backend running SELECT pg_sleep(30) to appear in pg_stat_activity'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT count(pg_terminate_backend(pid)) > 0 AS connection_killed FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(30)' AND state = 'active';
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT state FROM task WHERE "group" = 'connection_kill_recovery' AND input = 'SELECT pg_sleep(30)') NOT IN ('PLAN', 'TAKE', 'WORK') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''connection_kill_recovery'' (input ''SELECT pg_sleep(30)'') to leave PLAN/TAKE/WORK after its connection is killed'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state = 'FAIL' AS failed_cleanly, error IS NOT NULL AS has_error FROM task WHERE "group" = 'connection_kill_recovery' AND input = 'SELECT pg_sleep(30)';
INSERT INTO task ("group", input, remote) VALUES ('connection_kill_recovery', 'SELECT 1 AS a', 'dbname=' || :'DBNAME');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'connection_kill_recovery' AND input = 'SELECT 1 AS a' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''connection_kill_recovery'' (input ''SELECT 1 AS a'') to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state = 'DONE' AS worker_recovered FROM task WHERE "group" = 'connection_kill_recovery' AND input = 'SELECT 1 AS a';
