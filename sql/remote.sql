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
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
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
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '8' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count, remote) SELECT '9', 'SELECT pg_sleep(1) AS a', 1, 6, 'application_name=test' FROM s;
INSERT INTO task ("group", input, max, count, remote) VALUES ('9', 'SELECT pg_sleep(1) AS a', 2, 6, 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT
    (SELECT count(*) FROM task WHERE "group" = '9' AND max = 2 AND plan > :ct::timestamp AND state = 'DONE') = 1 AS max2_task_done,
    (SELECT count(DISTINCT pid) FROM task WHERE "group" = '9' AND max = 2 AND plan > :ct::timestamp) = 1 AS max2_single_worker,
    (SELECT count(*) FROM task WHERE "group" = '9' AND max = 1 AND plan > :ct::timestamp AND state = 'DONE') = 10 AS max1_all_done,
    (SELECT count(DISTINCT pid) FROM task WHERE "group" = '9' AND max = 1 AND plan > :ct::timestamp) >= 2 AS max1_multiple_workers;
WITH s AS (SELECT generate_series(1, 20) AS s) INSERT INTO task ("group", input, max, count, remote) SELECT '10', 'SELECT pg_sleep(1) AS a', 1, 5, 'application_name=test' FROM s;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '10' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count, active, remote) SELECT '11', 'SELECT pg_sleep(10) AS a', 1, 5, '5 sec', 'application_name=test' FROM s;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '11' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state ORDER BY 6;
INSERT INTO task ("group", input, remote) VALUES ('12', 'SELECT 1', 'application_name');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '12' AND plan > :ct::timestamp;
SELECT quote_literal(clock_timestamp()) AS ct13 \gset
WITH s AS (SELECT generate_series(1, 3) AS s) INSERT INTO task ("group", input, max, remote) SELECT '13', 'SELECT clock_timestamp() AS a', -3000, 'application_name=test' FROM s;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", min(start) - :ct13::timestamptz < interval '2500 ms' AS first_run_immediate, bool_and(gap IS NULL OR gap >= interval '3 sec') AS pause_ok FROM (
    SELECT "group", start, plan - lag(plan) OVER (ORDER BY plan) AS gap FROM task WHERE "group" = '13' AND plan > :ct::timestamp
) x GROUP BY "group";
SELECT quote_literal(clock_timestamp()) AS ct14 \gset
WITH s AS (SELECT generate_series(1, 3) AS s) INSERT INTO task ("group", input, max, drift, remote) SELECT '14', 'SELECT clock_timestamp() AS a', -3000, true, 'application_name=test' FROM s;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", min(start) - :ct14::timestamptz < interval '2500 ms' AS first_run_immediate, bool_and(gap IS NULL OR gap >= interval '3 sec') AS pause_ok FROM (
    SELECT "group", start, plan - lag(plan) OVER (ORDER BY plan) AS gap FROM task WHERE "group" = '14' AND plan > :ct::timestamp
) x GROUP BY "group";
WITH s AS (SELECT generate_series(1, 8) AS s) INSERT INTO task ("group", input, live, remote) SELECT '15', 'SELECT pg_sleep(0.3) AS a', '2 sec', 'application_name=test' FROM s;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", count(DISTINCT pid) > 1 AS multiple_workers, max(cnt) > 1 AS reuse_happened FROM (
    SELECT "group", pid, count(*) AS cnt FROM task WHERE "group" = '15' AND plan > :ct::timestamp GROUP BY "group", pid
) x GROUP BY "group";
INSERT INTO task ("group", input, quote, escape, remote) VALUES ('16', $task$SELECT 'a' || '"' || 'b' || chr(92) || 'c' AS a$task$, '"', '\', 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '16' AND plan > :ct::timestamp;
INSERT INTO task ("group", input, remote) VALUES ($task$it's a \group$task$, 'SELECT current_setting(''pg_task.group'') AS a', 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT state, "group" OPERATOR(pg_catalog.=) output AS group_roundtrip_ok FROM task WHERE "group" = $task$it's a \group$task$ AND plan > :ct::timestamp;
INSERT INTO task ("group", input, remote) VALUES ('17', 'DELETE FROM task WHERE false', 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '17' AND plan > :ct::timestamp;
SET client_min_messages = warning;
CREATE TABLE copy_probe (a int);
RESET client_min_messages;
INSERT INTO task ("group", input, remote) VALUES ('18', 'COPY (SELECT 1) TO STDOUT', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('19', 'COPY copy_probe FROM STDIN', 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '18' AND plan > :ct::timestamp;
SELECT "group", input, output, error, state FROM task WHERE "group" = '19' AND plan > :ct::timestamp;
DROP TABLE copy_probe;
INSERT INTO task ("group", input, remote) VALUES ('20', 'COPY (SELECT generate_series(1, 100000)) TO STDOUT', 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
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
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group",
    bool_and(CASE WHEN input = 'SELECT pg_sleep(3)' THEN state = 'FAIL' AND error LIKE '%statement timeout%' ELSE state = 'DONE' END) AS timeout_leak_fixed,
    count(DISTINCT pid) = 1 AS same_connection_reused
FROM task WHERE "group" = '21' AND plan > :ct::timestamp GROUP BY "group";
INSERT INTO task ("group", input, remote) VALUES ('22', 'SELECT current_setting(''pg_task.schema'') || ''.'' || current_setting(''pg_task.table'') AS a', 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '22' AND plan > :ct::timestamp;
DELETE FROM task WHERE "group" IN ('24', '25');
INSERT INTO task ("group", input, repeat, remote) VALUES ('24', 'SELECT pg_sleep(1) AS a', '3 sec', 'application_name=test');
INSERT INTO task ("group", input, repeat, drift, remote) VALUES ('25', 'SELECT pg_sleep(1) AS a', '3 sec', true, 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..90 LOOP
        IF (SELECT count(*) FILTER (WHERE "group" = '24') >= 3 AND count(*) FILTER (WHERE "group" = '25') >= 3 FROM task WHERE "group" IN ('24', '25') AND state = 'DONE') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" IN ('24', '25') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('24', '25') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('24', '25') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('24', '25') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('24', '25') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('24', '25') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('24', '25') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('24', '25') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('24', '25') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('24', '25') AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" IN ('24', '25') AND state = 'PLAN';
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task WHERE "group" IN ('24', '25') AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" IN ('24', '25') AND state NOT IN ('DONE', 'GONE', 'FAIL');
WITH g AS (
    SELECT id, parent, lag(id) OVER (ORDER BY plan) AS prev_id, plan - lag(plan) OVER (ORDER BY plan) AS gap
    FROM task WHERE "group" = '24' AND plan > :ct::timestamp
)
SELECT count(*) >= 3 AS repeated_enough,
    bool_and(parent IS NOT DISTINCT FROM prev_id) AS parent_chain_ok,
    bool_and(gap IS NULL OR LEAST(extract(epoch FROM gap)::numeric % 3, 3 - extract(epoch FROM gap)::numeric % 3) < 0.5) AS grid_aligned
FROM g;
WITH g AS (
    SELECT id, parent, lag(id) OVER (ORDER BY plan) AS prev_id, plan - lag(stop) OVER (ORDER BY plan) AS gap
    FROM task WHERE "group" = '25' AND plan > :ct::timestamp
)
SELECT count(*) >= 3 AS repeated_enough,
    bool_and(parent IS NOT DISTINCT FROM prev_id) AS parent_chain_ok,
    bool_and(gap IS NULL OR gap BETWEEN interval '2900 ms' AND interval '3500 ms') AS drift_from_stop_ok
FROM g;
DELETE FROM task WHERE "group" = '26';
INSERT INTO task ("group", input, remote) VALUES ('26', 'INSERT INTO task ("group", input, remote) VALUES (''26'', ''SELECT 1 AS a'', ''application_name=test'')', 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '26') >= 2 AND (SELECT count(*) FROM task WHERE "group" = '26' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
WITH g AS (
    SELECT id, parent, input, state FROM task WHERE "group" = '26' AND plan > :ct::timestamp
)
SELECT count(*) = 2 AS both_rows_present,
    bool_and(state = 'DONE') AS both_done,
    (SELECT parent FROM g WHERE input LIKE 'SELECT%') = (SELECT id FROM g WHERE input LIKE 'INSERT%') AS chaining_ok
FROM g;
DELETE FROM task WHERE "group" = '27';
INSERT INTO task ("group", input, remote, timeout) VALUES ('27', 'SELECT pg_sleep(30)', 'application_name=test', '1 min');
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF EXISTS (SELECT 1 FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(30)' AND state = 'active') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT count(pg_terminate_backend(pid)) > 0 AS connection_killed FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(30)' AND state = 'active';
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT state FROM task WHERE "group" = '27' AND input = 'SELECT pg_sleep(30)') NOT IN ('PLAN', 'TAKE', 'WORK') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT state = 'FAIL' AS failed_cleanly, error IS NOT NULL AS has_error FROM task WHERE "group" = '27' AND input = 'SELECT pg_sleep(30)';
INSERT INTO task ("group", input, remote) VALUES ('27', 'SELECT 1 AS a', 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '27' AND input = 'SELECT 1 AS a' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT state = 'DONE' AS worker_recovered FROM task WHERE "group" = '27' AND input = 'SELECT 1 AS a';
DELETE FROM task WHERE "group" = '28';
INSERT INTO task ("group", input, repeat, remote) VALUES ('28', 'SELECT pg_sleep(30)', '3 sec', 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..90 LOOP
        IF (SELECT state FROM task WHERE "group" = '28' AND parent IS NULL) = 'WORK' THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
UPDATE task SET state = 'STOP' WHERE "group" = '28' AND parent IS NULL AND state = 'WORK';
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT state FROM task WHERE "group" = '28' AND parent IS NULL) = 'STOP' THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) = 1 AS no_repeat_after_stop, bool_and(state = 'STOP') AS cancelled_cleanly FROM task WHERE "group" = '28';
DELETE FROM task WHERE "group" = '28'; -- a STOP row is terminal and never cleaned up by pg_task itself; leaving it behind would make every later "wait for everything to finish" loop in this and other test files spin out to its full bound
SET pg_task.header = false;
SET pg_task.string = false;
SET pg_task.delimiter = ',';
SET pg_task."null" = '<NULL>';
SET pg_task.quote = '"';
DELETE FROM task WHERE "group" = '29';
INSERT INTO task ("group", input, remote) VALUES ('29', 'SELECT 1 AS a, ''text'' AS b, NULL::int AS c', 'application_name=test');
RESET pg_task.header;
RESET pg_task.string;
RESET pg_task.delimiter;
RESET pg_task."null";
RESET pg_task.quote;
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '29' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT output = '"1","text",<NULL>' AS format_ok FROM task WHERE "group" = '29' AND plan > :ct::timestamp;
DELETE FROM task WHERE "group" IN ('30', '31', '32');
ALTER SYSTEM SET pg_task."limit" = 1;
SELECT pg_reload_conf();
INSERT INTO task ("group", input, remote) VALUES ('30', 'SELECT pg_sleep(5) AS a', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('31', 'SELECT pg_sleep(5) AS a', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('32', 'SELECT pg_sleep(5) AS a', 'application_name=test');
DO $$ BEGIN PERFORM pg_sleep(2); END $$;
SELECT count(*) FILTER (WHERE state != 'PLAN') >= 1 AS some_dispatched, count(*) FILTER (WHERE state = 'PLAN') >= 1 AS some_capped FROM task WHERE "group" IN ('30', '31', '32') AND plan > :ct::timestamp;
ALTER SYSTEM RESET pg_task."limit";
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task WHERE "group" IN ('30', '31', '32') AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" IN ('30', '31', '32');
ALTER SYSTEM SET pg_task.run = 1;
SELECT pg_reload_conf();
INSERT INTO task ("group", input, remote) VALUES ('30', 'SELECT pg_sleep(5) AS a', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('31', 'SELECT pg_sleep(5) AS a', 'application_name=test');
INSERT INTO task ("group", input, remote) VALUES ('32', 'SELECT pg_sleep(5) AS a', 'application_name=test');
DO $$ BEGIN PERFORM pg_sleep(2); END $$;
SELECT count(*) FILTER (WHERE state != 'PLAN') >= 1 AS some_dispatched, count(*) FILTER (WHERE state = 'PLAN') >= 1 AS some_capped FROM task WHERE "group" IN ('30', '31', '32') AND plan > :ct::timestamp;
ALTER SYSTEM RESET pg_task.run;
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task WHERE "group" IN ('30', '31', '32') AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" = '33';
INSERT INTO task ("group", input, remote) VALUES ('33', 'BEGIN; SELECT 1 AS a', 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '33' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT output, error, state FROM task WHERE "group" = '33' AND plan > :ct::timestamp;
DELETE FROM task WHERE "group" = '34';
INSERT INTO task ("group", input, remote) VALUES ('34', $task$DO $inner$ BEGIN RAISE EXCEPTION 'boom' USING DETAIL = 'detail text', HINT = 'hint text'; END $inner$;$task$, 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '34' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT output, error, state FROM task WHERE "group" = '34' AND plan > :ct::timestamp;
ALTER SYSTEM SET log_error_verbosity = 'verbose';
SELECT pg_reload_conf();
SET check_function_bodies = off;
CREATE FUNCTION query_location_probe() RETURNS int LANGUAGE SQL AS $probe$SELECT SELEKT 1$probe$;
RESET check_function_bodies;
DELETE FROM task WHERE "group" = '35';
INSERT INTO task ("group", input, remote) VALUES ('35', 'SELECT query_location_probe()', 'application_name=test');
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '35' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT error LIKE '%QUERY:  SELECT SELEKT 1%' AS query_field_ok, error LIKE '%LOCATION:  %, %:%' AS location_field_ok, state FROM task WHERE "group" = '35' AND plan > :ct::timestamp;
DROP FUNCTION query_location_probe();
ALTER SYSTEM RESET log_error_verbosity;
SELECT pg_reload_conf();
SET client_min_messages = warning;
CREATE ROLE task_remote_nopass_test LOGIN;
RESET client_min_messages;
GRANT CREATE ON DATABASE :"DBNAME" TO task_remote_nopass_test;
ALTER ROLE task_remote_nopass_test SET pg_task.schema = 'remote_nopass_test_schema';
SELECT '[{"data":"' || :'DBNAME' || '"},{"data":"' || :'DBNAME' || '","user":"task_remote_nopass_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF to_regclass('remote_nopass_test_schema.task') IS NOT NULL THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT (SELECT count(*) FROM pg_catalog.pg_settings WHERE name = 'gp_role') > 0 AS is_gp
\gset
SELECT '/tmp/pg_task_gp_policy_' || pg_backend_pid() || '.sql' AS gp_policy_file
\gset
\o :gp_policy_file
SELECT CASE WHEN :'is_gp' = 't' THEN 'SELECT NOT EXISTS (SELECT 1 FROM gp_dist_random(' || chr(39) || 'pg_class' || chr(39) || ') WHERE oid = ' || chr(39) || 'remote_nopass_test_schema.task' || chr(39) || '::regclass) AS need_gp_utility' ELSE 'SELECT false AS need_gp_utility' END;
SELECT '\gset';
\o
\i :gp_policy_file
SELECT '/tmp/pg_task_gp_utility_' || pg_backend_pid() || '.sql' AS gp_utility_file
\gset
\o :gp_utility_file
SELECT CASE WHEN :'need_gp_utility' = 't' THEN '\connect "dbname=' || :'DBNAME' || ' options=' || chr(39) || '-c gp_session_role=utility' || chr(39) || '"' ELSE '' END;
\o
\i :gp_utility_file
GRANT INSERT ON remote_nopass_test_schema.task TO task_remote_nopass_test;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE remote_nopass_test_schema.task_id_seq TO task_remote_nopass_test;
\connect :DBNAME
SET ROLE task_remote_nopass_test;
INSERT INTO remote_nopass_test_schema.task ("group", input, remote) VALUES ('nopass', 'SELECT 1 AS a', 'application_name=test');
RESET ROLE;
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM remote_nopass_test_schema.task WHERE "group" = 'nopass' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT state = 'FAIL' AS rejected_without_password, error LIKE '%password is required%' AS password_error_ok FROM remote_nopass_test_schema.task WHERE "group" = 'nopass';
ALTER SYSTEM RESET pg_task.json;
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE usename = 'task_remote_nopass_test') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
\i :gp_utility_file
SET client_min_messages TO WARNING;
DROP SCHEMA remote_nopass_test_schema CASCADE;
RESET client_min_messages;
\connect :DBNAME
REVOKE CREATE ON DATABASE :"DBNAME" FROM task_remote_nopass_test;
DROP ROLE task_remote_nopass_test;
SET client_min_messages = warning;
CREATE ROLE task_remote_author_nopass_test LOGIN;
RESET client_min_messages;
\i :gp_utility_file
GRANT INSERT ON task TO task_remote_author_nopass_test;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE task_id_seq TO task_remote_author_nopass_test;
\connect :DBNAME
DELETE FROM task WHERE "group" = 'author_nopass';
SET ROLE task_remote_author_nopass_test;
INSERT INTO task ("group", input, remote) VALUES ('author_nopass', 'SELECT 1 AS a', 'application_name=test');
RESET ROLE;
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'author_nopass' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT state = 'FAIL' AS rejected_without_password_for_unprivileged_author, error LIKE '%password is required%' AS password_error_ok FROM task WHERE "group" = 'author_nopass' AND plan > :ct::timestamp;
DELETE FROM task WHERE "group" = 'author_nopass';
\i :gp_utility_file
REVOKE INSERT ON task FROM task_remote_author_nopass_test;
REVOKE USAGE, SELECT, UPDATE ON SEQUENCE task_id_seq FROM task_remote_author_nopass_test;
\connect :DBNAME
DROP ROLE task_remote_author_nopass_test;
DELETE FROM task WHERE plan > :ct::timestamp; -- catch-all: remove anything this run inserted that an earlier per-group DELETE missed
