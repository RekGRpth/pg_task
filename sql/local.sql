\unset ECHO
\set QUIET 1
\pset format unaligned
\pset tuples_only true
\pset pager off
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input) VALUES ('0', 'SELECT 1 AS a WHERE false');
INSERT INTO task ("group", input) VALUES ('1', 'SELECT 1/0 AS a');
INSERT INTO task ("group", input) VALUES ('2', 'SELECT 1 AS a');
INSERT INTO task ("group", input) VALUES ('3', 'SELECT 1 AS a, 2 AS b');
INSERT INTO task ("group", input) VALUES ('4', 'SELECT 1 AS a;SELECT 2 AS b');
INSERT INTO task ("group", input) VALUES ('5', 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c');
INSERT INTO task ("group", input) VALUES ('6', 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c, 4 AS d');
WITH s AS (SELECT generate_series(1,10) AS s)  INSERT INTO task ("group", input, max, live) SELECT '7', 'SELECT pg_sleep(0.1) AS a', 2, '1 min' FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, state FROM task WHERE "group" = '0' AND input = 'SELECT 1 AS a WHERE false' AND plan > :ct::timestamp;
SELECT "group", input, output, state FROM task WHERE "group" = '1' AND input = 'SELECT 1/0 AS a' AND plan > :ct::timestamp;
SELECT "group", input, output, state FROM task WHERE "group" = '2' AND input = 'SELECT 1 AS a' AND plan > :ct::timestamp;
SELECT "group", input, output, state FROM task WHERE "group" = '3' AND input = 'SELECT 1 AS a, 2 AS b' AND plan > :ct::timestamp;
SELECT "group", input, output, state FROM task WHERE "group" = '4' AND input = 'SELECT 1 AS a;SELECT 2 AS b' AND plan > :ct::timestamp;
SELECT "group", input, output, state FROM task WHERE "group" = '5' AND input = 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c' AND plan > :ct::timestamp;
SELECT "group", input, output, state FROM task WHERE "group" = '6' AND input = 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c, 4 AS d' AND plan > :ct::timestamp;
SELECT "group", input, output, state, count(id) FROM task WHERE "group" = '7' AND input = 'SELECT pg_sleep(0.1) AS a' AND plan > :ct::timestamp GROUP BY "group", input, output, state, pid;
