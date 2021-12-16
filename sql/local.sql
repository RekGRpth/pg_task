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
INSERT INTO task ("group", input) VALUES ('0', 'SELECT 1 AS a WHERE false');
INSERT INTO task ("group", input) VALUES ('1', 'SELECT 1/0 AS a');
INSERT INTO task ("group", input, timeout) VALUES ('2', 'SELECT pg_sleep(2) AS a', '1 sec');
INSERT INTO task ("group", input) VALUES ('3', 'SELECT 1 AS a');
INSERT INTO task ("group", input) VALUES ('4', 'SELECT 1 AS a, 2 AS b');
INSERT INTO task ("group", input) VALUES ('5', 'SELECT 1 AS a;SELECT 2 AS b');
INSERT INTO task ("group", input) VALUES ('6', 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c');
INSERT INTO task ("group", input) VALUES ('7', 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c, 4 AS d');
COMMIT;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state != 'DONE') = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '0' AND plan > :ct::timestamp;
SELECT "group", input, output, error, state FROM task WHERE "group" = '1' AND plan > :ct::timestamp;
SELECT "group", input, output, error, state FROM task WHERE "group" = '2' AND plan > :ct::timestamp;
SELECT "group", input, output, error, state FROM task WHERE "group" = '3' AND plan > :ct::timestamp;
SELECT "group", input, output, error, state FROM task WHERE "group" = '4' AND plan > :ct::timestamp;
SELECT "group", input, output, error, state FROM task WHERE "group" = '5' AND plan > :ct::timestamp;
SELECT "group", input, output, error, state FROM task WHERE "group" = '6' AND plan > :ct::timestamp;
SELECT "group", input, output, error, state FROM task WHERE "group" = '7' AND plan > :ct::timestamp;
BEGIN;
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count) SELECT '8', 'SELECT pg_sleep(1) AS a', 2, 5 FROM s;
COMMIT;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state != 'DONE') = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '8' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
BEGIN;
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count) SELECT '9', 'SELECT pg_sleep(1) AS a', 2, 6 FROM s;
INSERT INTO task ("group", input, max, count) VALUES ('9', 'SELECT pg_sleep(1) AS a', 3, 6);
COMMIT;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state != 'DONE') = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, max, count(id) FROM task WHERE "group" = '9' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, max, pid ORDER BY max DESC;
BEGIN;
WITH s AS (SELECT generate_series(1, 20) AS s) INSERT INTO task ("group", input, max, count) SELECT '10', 'SELECT pg_sleep(1) AS a', 2, 5 FROM s;
COMMIT;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state != 'DONE') = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '10' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
BEGIN;
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count, active) SELECT '11', 'SELECT pg_sleep(10) AS a', 2, 5, '5 sec' FROM s;
COMMIT;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state != 'DONE') = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '11' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
