\unset ECHO
\set QUIET 1
\pset format unaligned
\pset tuples_only true
\pset pager off
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, remote) VALUES ('0', 'SELECT 1 AS a WHERE false', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('1', 'SELECT 1/0 AS a', 'application_name=test');
INSERT INTO task ("group", input, timeout, remote) VALUES ('2', 'SELECT pg_sleep(2) AS a', '1 sec', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('3', 'SELECT 1 AS a', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('4', 'SELECT 1 AS a, 2 AS b', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('5', 'SELECT 1 AS a;SELECT 2 AS b', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('6', 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('7', 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c, 4 AS d', 'application_name=test');
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
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
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count, remote) SELECT '8', 'SELECT pg_sleep(1) AS a', 1, 5, 'application_name=test' FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '8' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count, remote) SELECT '9', 'SELECT pg_sleep(1) AS a', 1, 6, 'application_name=test' FROM s;
INSERT INTO task ("group", input, max, count, remote) VALUES ('9', 'SELECT pg_sleep(1) AS a', 2, 6, 'application_name=test');
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, max, count(id) FROM task WHERE "group" = '9' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, max, pid ORDER BY max DESC, 7;
WITH s AS (SELECT generate_series(1, 20) AS s) INSERT INTO task ("group", input, max, count, remote) SELECT '10', 'SELECT pg_sleep(1) AS a', 1, 5, 'application_name=test' FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '10' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count, active, remote) SELECT '11', 'SELECT pg_sleep(10) AS a', 1, 5, '5 sec', 'application_name=test' FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '11' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state ORDER BY 6;
INSERT INTO task ("group", input, remote) VALUES ('12', 'SELECT 1', 'application_name');
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '12' AND plan > :ct::timestamp;
