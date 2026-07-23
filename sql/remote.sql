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
SELECT quote_literal(clock_timestamp()) AS ct13 \gset
WITH s AS (SELECT generate_series(1, 3) AS s) INSERT INTO task ("group", input, max, remote) SELECT '13', 'SELECT clock_timestamp() AS a', -3000, 'application_name=test' FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", min(start) - :ct13::timestamptz < interval '2500 ms' AS first_run_immediate, bool_and(gap IS NULL OR gap BETWEEN interval '2 sec' AND interval '5 sec') AS pause_ok FROM (
    SELECT "group", start, start - lag(start) OVER (ORDER BY start) AS gap FROM task WHERE "group" = '13' AND plan > :ct::timestamp
) x GROUP BY "group";
SELECT quote_literal(clock_timestamp()) AS ct14 \gset
WITH s AS (SELECT generate_series(1, 3) AS s) INSERT INTO task ("group", input, max, drift, remote) SELECT '14', 'SELECT clock_timestamp() AS a', -3000, true, 'application_name=test' FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", min(start) - :ct14::timestamptz < interval '2500 ms' AS first_run_immediate, bool_and(gap IS NULL OR gap BETWEEN interval '2 sec' AND interval '5 sec') AS pause_ok FROM (
    SELECT "group", start, start - lag(start) OVER (ORDER BY start) AS gap FROM task WHERE "group" = '14' AND plan > :ct::timestamp
) x GROUP BY "group";
WITH s AS (SELECT generate_series(1, 8) AS s) INSERT INTO task ("group", input, live, remote) SELECT '15', 'SELECT pg_sleep(0.3) AS a', '2 sec', 'application_name=test' FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", count(DISTINCT pid) > 1 AS multiple_workers, max(cnt) > 1 AS reuse_happened FROM (
    SELECT "group", pid, count(*) AS cnt FROM task WHERE "group" = '15' AND plan > :ct::timestamp GROUP BY "group", pid
) x GROUP BY "group";
INSERT INTO task ("group", input, quote, escape, remote) VALUES ('16', $task$SELECT 'a' || '"' || 'b' || chr(92) || 'c' AS a$task$, '"', '\', 'application_name=test');
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '16' AND plan > :ct::timestamp;
INSERT INTO task ("group", input, remote) VALUES ($task$it's a \group$task$, 'SELECT current_setting(''pg_task.group'') AS a', 'application_name=test');
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT state, "group" OPERATOR(pg_catalog.=) output AS group_roundtrip_ok FROM task WHERE "group" = $task$it's a \group$task$ AND plan > :ct::timestamp;
INSERT INTO task ("group", input, remote) VALUES ('17', 'DELETE FROM task WHERE false', 'application_name=test');
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '17' AND plan > :ct::timestamp;
INSERT INTO task ("group", input, remote) VALUES ('18', 'COPY (SELECT 1) TO STDOUT', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('19', 'COPY task FROM STDIN', 'application_name=test');
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '18' AND plan > :ct::timestamp;
SELECT "group", input, output, error, state FROM task WHERE "group" = '19' AND plan > :ct::timestamp;
INSERT INTO task ("group", input, remote) VALUES ('20', 'COPY (SELECT generate_series(1, 100000)) TO STDOUT', 'application_name=test');
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", state,
    array_length(regexp_split_to_array(rtrim(output, chr(10)), chr(10)), 1) = 100000 AS row_count_ok,
    (regexp_split_to_array(rtrim(output, chr(10)), chr(10)))[1] = '1' AS first_row_ok,
    (regexp_split_to_array(rtrim(output, chr(10)), chr(10)))[100000] = '100000' AS last_row_ok
FROM task WHERE "group" = '20' AND plan > :ct::timestamp;
INSERT INTO task ("group", input, remote, save, count, timeout) VALUES ('21', 'SELECT pg_sleep(3)', 'application_name=test', true, 5, '1 sec');
INSERT INTO task ("group", input, remote, save, count) VALUES ('21', 'SELECT pg_sleep(2)', 'application_name=test', true, 5);
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group",
    bool_and(CASE WHEN input = 'SELECT pg_sleep(3)' THEN state = 'FAIL' AND error LIKE '%statement timeout%' ELSE state = 'DONE' END) AS timeout_leak_fixed,
    count(DISTINCT pid) = 1 AS same_connection_reused
FROM task WHERE "group" = '21' AND plan > :ct::timestamp GROUP BY "group";
