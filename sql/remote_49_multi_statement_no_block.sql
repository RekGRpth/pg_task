-- the first of several statements fills the remote server's send buffer and so reaches pg_work before the rest is done: pg_work must keep running other tasks meanwhile, not wait in PQgetResult for the rest
DELETE FROM task WHERE "group" IN ('no_block_remote', 'no_block_local');
INSERT INTO task ("group", input, remote) VALUES ('no_block_remote', 'SELECT repeat(''x'', 500000); SELECT pg_sleep(6)', 'dbname=' || :'DBNAME');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE a.query LIKE 'SELECT repeat(''x'', 500000); SELECT pg_sleep(6)' AND a.state = 'active' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN clock_timestamp() - a.query_start > '1 second' ELSE to_json(a) ->> 'wait_event' = 'PgSleep' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the remote task to reach its second statement'; END IF;
END;$body$ LANGUAGE plpgsql;
INSERT INTO task ("group", input) VALUES ('no_block_local', 'SELECT 1');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'no_block_local' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''no_block_local'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", state FROM task WHERE "group" IN ('no_block_remote', 'no_block_local') ORDER BY "group";
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'no_block_remote' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''no_block_remote'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, output = repeat('x', 500000) || chr(10) AS output_ok, error FROM task WHERE "group" = 'no_block_remote';
DELETE FROM task WHERE "group" IN ('no_block_remote', 'no_block_local');
