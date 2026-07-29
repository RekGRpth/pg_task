\unset ECHO
\set QUIET 1
\pset format unaligned
\pset tuples_only true
\pset pager off
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input) VALUES ('0', 'SELECT 1 AS a WHERE false');
INSERT INTO task ("group", input) VALUES ('1', 'SELECT 1/0 AS a');
INSERT INTO task ("group", input, timeout) VALUES ('2', 'SELECT pg_sleep(2) AS a', '1 sec');
INSERT INTO task ("group", input) VALUES ('3', 'SELECT 1 AS a');
INSERT INTO task ("group", input) VALUES ('4', 'SELECT 1 AS a, 2 AS b');
INSERT INTO task ("group", input) VALUES ('5', 'SELECT 1 AS a;SELECT 2 AS b');
INSERT INTO task ("group", input) VALUES ('6', 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c');
INSERT INTO task ("group", input) VALUES ('7', 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c, 4 AS d');
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
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count) SELECT '8', 'SELECT pg_sleep(1) AS a', 1, 5 FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '8' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count) SELECT '9', 'SELECT pg_sleep(1) AS a', 1, 6 FROM s;
INSERT INTO task ("group", input, max, count) VALUES ('9', 'SELECT pg_sleep(1) AS a', 2, 6);
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, max, count(id) FROM task WHERE "group" = '9' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, max, pid ORDER BY max DESC, 7;
WITH s AS (SELECT generate_series(1, 20) AS s) INSERT INTO task ("group", input, max, count) SELECT '10', 'SELECT pg_sleep(1) AS a', 1, 5 FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '10' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count, active) SELECT '11', 'SELECT pg_sleep(10) AS a', 1, 5, '5 sec' FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '11' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state ORDER BY 6;
SELECT quote_literal(clock_timestamp()) AS ct12 \gset
WITH s AS (SELECT generate_series(1, 3) AS s) INSERT INTO task ("group", input, max) SELECT '12', 'SELECT clock_timestamp() AS a', -3000 FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", min(start) - :ct12::timestamptz < interval '2500 ms' AS first_run_immediate, bool_and(gap IS NULL OR gap BETWEEN interval '2 sec' AND interval '5 sec') AS pause_ok FROM (
    SELECT "group", start, start - lag(start) OVER (ORDER BY start) AS gap FROM task WHERE "group" = '12' AND plan > :ct::timestamp
) x GROUP BY "group";
SELECT quote_literal(clock_timestamp()) AS ct13 \gset
WITH s AS (SELECT generate_series(1, 3) AS s) INSERT INTO task ("group", input, max, drift) SELECT '13', 'SELECT clock_timestamp() AS a', -3000, true FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", min(start) - :ct13::timestamptz < interval '2500 ms' AS first_run_immediate, bool_and(gap IS NULL OR gap BETWEEN interval '2 sec' AND interval '5 sec') AS pause_ok FROM (
    SELECT "group", start, start - lag(start) OVER (ORDER BY start) AS gap FROM task WHERE "group" = '13' AND plan > :ct::timestamp
) x GROUP BY "group";
WITH s AS (SELECT generate_series(1, 8) AS s) INSERT INTO task ("group", input, live) SELECT '14', 'SELECT pg_sleep(0.3) AS a', '2 sec' FROM s;
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", count(DISTINCT pid) > 1 AS multiple_workers, max(cnt) > 1 AS reuse_happened FROM (
    SELECT "group", pid, count(*) AS cnt FROM task WHERE "group" = '14' AND plan > :ct::timestamp GROUP BY "group", pid
) x GROUP BY "group";
INSERT INTO task ("group", input, quote, escape) VALUES ('15', $task$SELECT 'a' || '"' || 'b' || chr(92) || 'c' AS a$task$, '"', '\');
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '15' AND plan > :ct::timestamp;
INSERT INTO task ("group", input) VALUES ('16', 'DELETE FROM task WHERE false');
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '16' AND plan > :ct::timestamp;
INSERT INTO task ("group", input) VALUES ('17', 'COPY (SELECT 1) TO STDOUT');
INSERT INTO task ("group", input) VALUES ('18', 'COPY task FROM STDIN');
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '17' AND plan > :ct::timestamp;
SELECT "group", input, output, error, state FROM task WHERE "group" = '18' AND plan > :ct::timestamp;
INSERT INTO task ("group", input, count, save) VALUES ('19', 'CREATE TEMP TABLE save_probe_19 AS SELECT 1 AS a', 5, true);
INSERT INTO task ("group", input, count, save) VALUES ('19', 'SELECT count(*) FROM save_probe_19', 5, true);
INSERT INTO task ("group", input, count, save) VALUES ('20', 'CREATE TEMP TABLE save_probe_20 AS SELECT 1 AS a', 5, false);
INSERT INTO task ("group", input, count, save) VALUES ('20', 'SELECT count(*) FROM save_probe_20', 5, false);
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", count(DISTINCT pid) = 1 AS same_worker, max(output) FILTER (WHERE input LIKE 'SELECT count%') = '1' AS state_preserved
FROM task WHERE "group" = '19' AND plan > :ct::timestamp GROUP BY "group";
SELECT "group", bool_and(CASE WHEN input LIKE 'CREATE%' THEN state = 'DONE' ELSE state = 'FAIL' END) AS discard_worked, count(DISTINCT pid) = 1 AS same_worker
FROM task WHERE "group" = '20' AND plan > :ct::timestamp GROUP BY "group";
CREATE SCHEMA sp_probe_schema;
CREATE FUNCTION sp_probe_schema.search_path_probe() RETURNS text LANGUAGE sql AS $$ SELECT 'found'::text $$;
DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET search_path = sp_probe_schema, public', current_database()); END $$;
INSERT INTO task ("group", input) VALUES ('21', 'SELECT search_path_probe() AS a');
DO $body$ BEGIN
    WHILE true LOOP
        PERFORM pg_sleep(1);
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '21' AND plan > :ct::timestamp;
DO $$ BEGIN EXECUTE format('ALTER DATABASE %I RESET search_path', current_database()); END $$;
DROP FUNCTION sp_probe_schema.search_path_probe();
DROP SCHEMA sp_probe_schema;
DELETE FROM task WHERE "group" IN ('22', '23');
INSERT INTO task ("group", input, repeat) VALUES ('22', 'SELECT pg_sleep(1) AS a', '3 sec');
INSERT INTO task ("group", input, repeat, drift) VALUES ('23', 'SELECT pg_sleep(1) AS a', '3 sec', true);
DO $body$ BEGIN
    FOR i IN 1..90 LOOP
        IF (SELECT count(*) FILTER (WHERE "group" = '22') >= 3 AND count(*) FILTER (WHERE "group" = '23') >= 3 FROM task WHERE "group" IN ('22', '23') AND state = 'DONE') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" IN ('22', '23') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('22', '23') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('22', '23') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('22', '23') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('22', '23') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('22', '23') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('22', '23') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('22', '23') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('22', '23') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('22', '23') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('22', '23') AND state = 'PLAN';
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task WHERE "group" IN ('22', '23') AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" IN ('22', '23') AND state NOT IN ('DONE', 'GONE', 'FAIL');
WITH g AS (
    SELECT id, parent, lag(id) OVER (ORDER BY plan) AS prev_id, plan - lag(plan) OVER (ORDER BY plan) AS gap
    FROM task WHERE "group" = '22' AND plan > :ct::timestamp
)
SELECT count(*) >= 3 AS repeated_enough,
    bool_and(parent IS NOT DISTINCT FROM prev_id) AS parent_chain_ok,
    bool_and(gap IS NULL OR LEAST(extract(epoch FROM gap)::numeric % 3, 3 - extract(epoch FROM gap)::numeric % 3) < 0.5) AS grid_aligned
FROM g;
WITH g AS (
    SELECT id, parent, lag(id) OVER (ORDER BY plan) AS prev_id, plan - lag(stop) OVER (ORDER BY plan) AS gap
    FROM task WHERE "group" = '23' AND plan > :ct::timestamp
)
SELECT count(*) >= 3 AS repeated_enough,
    bool_and(parent IS NOT DISTINCT FROM prev_id) AS parent_chain_ok,
    bool_and(gap IS NULL OR gap BETWEEN interval '2900 ms' AND interval '3500 ms') AS drift_from_stop_ok
FROM g;
DELETE FROM task WHERE "group" = '24';
INSERT INTO task ("group", input) VALUES ('24', 'INSERT INTO task ("group", input) VALUES (''24'', ''SELECT 1 AS a'')');
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '24') >= 2 AND (SELECT count(*) FROM task WHERE "group" = '24' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
WITH g AS (
    SELECT id, parent, input, state FROM task WHERE "group" = '24' AND plan > :ct::timestamp
)
SELECT count(*) = 2 AS both_rows_present,
    bool_and(state = 'DONE') AS both_done,
    (SELECT parent FROM g WHERE input LIKE 'SELECT%') = (SELECT id FROM g WHERE input LIKE 'INSERT%') AS chaining_ok
FROM g;
DELETE FROM task WHERE "group" = '25';
INSERT INTO task ("group", input) VALUES ('25', 'SELECT pg_sleep(10) AS a');
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT state FROM task WHERE "group" = '25') = 'WORK' THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT pid AS orig_pid FROM task WHERE "group" = '25'
\gset
SELECT count(pg_terminate_backend(:orig_pid)) > 0 AS worker_killed;
ALTER SYSTEM SET pg_task.reset = '2 sec';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT state FROM task WHERE "group" = '25') = 'DONE' THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM RESET pg_task.reset;
SELECT pg_reload_conf();
SELECT state = 'DONE' AS recovered, pid != :orig_pid AS pid_changed FROM task WHERE "group" = '25';
