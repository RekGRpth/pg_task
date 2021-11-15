\unset ECHO
\set QUIET 1
\pset format unaligned
\pset tuples_only true
--\pset footer off
\pset pager off
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true
--\set my now()

SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset

--\echo :ct
--\pset

--SELECT :"my";

--BEGIN;

INSERT INTO task (input) VALUES ('SELECT 1/0 AS a1');

INSERT INTO task (input) VALUES ('SELECT 1 AS a2');
INSERT INTO task (input) VALUES ('SELECT 1 AS a3, 2 AS b3');
INSERT INTO task (input) VALUES ('SELECT 1 AS a4;SELECT 2 AS b4');
INSERT INTO task (input) VALUES ('SELECT 1 AS a5, 2 AS b5;SELECT 3 AS c5');
INSERT INTO task (input) VALUES ('SELECT 1 AS a6, 2 AS b6;SELECT 3 AS c6, 4 AS d6');

--INSERT INTO task (input) VALUES ('SELECT 1 AS a WHERE false');
--INSERT INTO task (input) VALUES ('SELECT null AS a');

--WITH s AS (SELECT generate_series(1,10)) INSERT INTO task (input, max) VALUES ('SELECT 1 AS a7', 2) FROM s;
WITH s AS (SELECT generate_series(1,10) AS s)  INSERT INTO task (input, max, live) SELECT 'SELECT pg_sleep(1) AS a7', 2, '1 min' FROM s;

--COMMIT;

DO $body$ <<local>> DECLARE
    count bigint;
BEGIN
    WHILE true LOOP
        SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'FAIL') INTO local.count;
        IF local.count = 0 THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;

SELECT input, output, state FROM task WHERE input = 'SELECT 1/0 AS a1' AND start >= :ct::timestamp ORDER BY id desc;

SELECT input, output, state FROM task WHERE input = 'SELECT 1 AS a2' AND start >= :ct::timestamp ORDER BY id desc;
SELECT input, output, state FROM task WHERE input = 'SELECT 1 AS a3, 2 AS b3' AND start >= :ct::timestamp ORDER BY id desc;
SELECT input, output, state FROM task WHERE input = 'SELECT 1 AS a4;SELECT 2 AS b4' AND start >= :ct::timestamp ORDER BY id desc;
SELECT input, output, state FROM task WHERE input = 'SELECT 1 AS a5, 2 AS b5;SELECT 3 AS c5' AND start >= :ct::timestamp ORDER BY id desc;
SELECT input, output, state FROM task WHERE input = 'SELECT 1 AS a6, 2 AS b6;SELECT 3 AS c6, 4 AS d6' AND start >= :ct::timestamp ORDER BY id desc;

--SELECT input, output, state FROM task WHERE input = 'SELECT 1 AS a WHERE false' ORDER BY id desc LIMIT 1;
--SELECT input, output, state FROM task WHERE input = 'SELECT null AS a' ORDER BY id desc LIMIT 1;

--SELECT input, output, state, count(pid) AS pid FROM task WHERE input like 'SELECT % AS a7' AND start >= :ct::timestamp ORDER BY id desc LIMIT 1;
SELECT input, output, state, count(id) FROM task WHERE input like 'SELECT pg_sleep(1) AS a7' AND start >= :ct::timestamp GROUP BY input, output, state, pid;

--SELECT :"my";
