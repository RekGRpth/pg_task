DELETE FROM task WHERE "group" = 'connection_reuse_timeout_leak';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, remote, save, count, timeout) VALUES ('connection_reuse_timeout_leak', 'SELECT pg_sleep(3)', 'dbname=' || :'DBNAME', true, 5, '1 sec');
INSERT INTO task ("group", input, remote, save, count) VALUES ('connection_reuse_timeout_leak', 'SELECT pg_sleep(2)', 'dbname=' || :'DBNAME', true, 5);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'connection_reuse_timeout_leak' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''connection_reuse_timeout_leak'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group",
    bool_and(CASE WHEN input = 'SELECT pg_sleep(3)' THEN state = 'FAIL' AND error LIKE '%statement timeout%' ELSE state = 'DONE' END) AS timeout_leak_fixed,
    count(DISTINCT pid) = 1 AS same_connection_reused
FROM task WHERE "group" = 'connection_reuse_timeout_leak' AND plan > :ct::timestamp GROUP BY "group";
