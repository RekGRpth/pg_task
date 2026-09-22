CREATE TABLE deadlock_probe (id int PRIMARY KEY, val int);
INSERT INTO deadlock_probe VALUES (1, 0), (2, 0);
ALTER SYSTEM SET deadlock_timeout = '100ms';
SELECT pg_reload_conf();
DELETE FROM task WHERE "group" IN ('deadlock_a', 'deadlock_b');
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input) VALUES ('deadlock_a', 'UPDATE deadlock_probe SET val = 1 WHERE id = 1; SELECT pg_sleep(2); UPDATE deadlock_probe SET val = 1 WHERE id = 2');
INSERT INTO task ("group", input) VALUES ('deadlock_b', 'UPDATE deadlock_probe SET val = 1 WHERE id = 2; SELECT pg_sleep(2); UPDATE deadlock_probe SET val = 1 WHERE id = 1');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" IN ('deadlock_a', 'deadlock_b') AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 15 x pg_sleep(1) waiting for task groups ''deadlock_a'', ''deadlock_b'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) FILTER (WHERE state = 'FAIL' AND error LIKE '%deadlock detected%' AND error ~ 'Process \d+: ') = 1 AS deadlock_log_detail_ok, count(*) FILTER (WHERE state = 'DONE') = 1 AS other_committed FROM task WHERE "group" IN ('deadlock_a', 'deadlock_b') AND plan > :ct::timestamp;
ALTER SYSTEM RESET deadlock_timeout;
SELECT pg_reload_conf();
DROP TABLE deadlock_probe;
