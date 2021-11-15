\unset ECHO
\set QUIET 1
\pset format unaligned
\pset tuples_only true
\pset pager off
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
BEGIN;
INSERT INTO task ("group", input, remote) VALUES ('0', 'SELECT 1 AS a WHERE false', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('1', 'SELECT 1/0 AS a', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('2', 'SELECT 1 AS a', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('3', 'SELECT 1 AS a, 2 AS b', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('4', 'SELECT 1 AS a;SELECT 2 AS b', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('5', 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('6', 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c, 4 AS d', 'application_name=test');
WITH s AS (SELECT generate_series(1,10) AS s) INSERT INTO task ("group", input, max, live, remote) SELECT '7', 'SELECT pg_sleep(0.1) AS a', 2, '1 min', 'application_name=test' FROM s;
COMMIT;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(0.1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, state FROM task WHERE "group" = '0' AND plan > :ct::timestamp;
SELECT "group", input, output, state FROM task WHERE "group" = '1' AND plan > :ct::timestamp;
SELECT "group", input, output, state FROM task WHERE "group" = '2' AND plan > :ct::timestamp;
SELECT "group", input, output, state FROM task WHERE "group" = '3' AND plan > :ct::timestamp;
SELECT "group", input, output, state FROM task WHERE "group" = '4' AND plan > :ct::timestamp;
SELECT "group", input, output, state FROM task WHERE "group" = '5' AND plan > :ct::timestamp;
SELECT "group", input, output, state FROM task WHERE "group" = '6' AND plan > :ct::timestamp;
SELECT "group", input, state, count(id) FROM task WHERE "group" = '7' AND plan > :ct::timestamp GROUP BY "group", input, output, state, pid;
BEGIN;
WITH s AS (SELECT generate_series(1,10) AS s) INSERT INTO task ("group", input, max, live, remote) SELECT '8', 'SELECT pg_sleep(0.1) AS a', 2, '1 min', 'application_name=test' FROM s;
INSERT INTO task ("group", input, max, live, remote) VALUES ('8', 'SELECT pg_sleep(0.1) AS a', 3, '1 min', 'application_name=test');
COMMIT;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(0.1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, state, max, count(id) FROM task WHERE "group" = '8' AND plan > :ct::timestamp GROUP BY "group", input, output, state, max, pid ORDER BY max DESC;
