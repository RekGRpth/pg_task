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
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count) SELECT '8', 'SELECT pg_sleep(1) AS a', 1, 5 FROM s;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '8' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count) SELECT '9', 'SELECT pg_sleep(1) AS a', 1, 6 FROM s;
INSERT INTO task ("group", input, max, count) VALUES ('9', 'SELECT pg_sleep(1) AS a', 2, 6);
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
WITH s AS (SELECT generate_series(1, 20) AS s) INSERT INTO task ("group", input, max, count) SELECT '10', 'SELECT pg_sleep(1) AS a', 1, 5 FROM s;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '10' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count, active) SELECT '11', 'SELECT pg_sleep(10) AS a', 1, 5, '5 sec' FROM s;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = '11' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state ORDER BY 6;
SELECT quote_literal(clock_timestamp()) AS ct12 \gset
WITH s AS (SELECT generate_series(1, 3) AS s) INSERT INTO task ("group", input, max) SELECT '12', 'SELECT clock_timestamp() AS a', -3000 FROM s;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", min(start) - :ct12::timestamptz < interval '2500 ms' AS first_run_immediate, bool_and(gap IS NULL OR gap BETWEEN interval '2 sec' AND interval '5 sec') AS pause_ok FROM (
    SELECT "group", start, start - lag(start) OVER (ORDER BY start) AS gap FROM task WHERE "group" = '12' AND plan > :ct::timestamp
) x GROUP BY "group";
SELECT quote_literal(clock_timestamp()) AS ct13 \gset
WITH s AS (SELECT generate_series(1, 3) AS s) INSERT INTO task ("group", input, max, drift) SELECT '13', 'SELECT clock_timestamp() AS a', -3000, true FROM s;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", min(start) - :ct13::timestamptz < interval '2500 ms' AS first_run_immediate, bool_and(gap IS NULL OR gap BETWEEN interval '2 sec' AND interval '5 sec') AS pause_ok FROM (
    SELECT "group", start, start - lag(start) OVER (ORDER BY start) AS gap FROM task WHERE "group" = '13' AND plan > :ct::timestamp
) x GROUP BY "group";
WITH s AS (SELECT generate_series(1, 8) AS s) INSERT INTO task ("group", input, live) SELECT '14', 'SELECT pg_sleep(0.3) AS a', '2 sec' FROM s;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", count(DISTINCT pid) > 1 AS multiple_workers, max(cnt) > 1 AS reuse_happened FROM (
    SELECT "group", pid, count(*) AS cnt FROM task WHERE "group" = '14' AND plan > :ct::timestamp GROUP BY "group", pid
) x GROUP BY "group";
INSERT INTO task ("group", input, quote, escape) VALUES ('15', $task$SELECT 'a' || '"' || 'b' || chr(92) || 'c' AS a$task$, '"', '\');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '15' AND plan > :ct::timestamp;
INSERT INTO task ("group", input) VALUES ('16', 'DELETE FROM task WHERE false');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '16' AND plan > :ct::timestamp;
INSERT INTO task ("group", input) VALUES ('17', 'COPY (SELECT 1) TO STDOUT');
INSERT INTO task ("group", input) VALUES ('18', 'COPY task FROM STDIN');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = '17' AND plan > :ct::timestamp;
SELECT "group", input, output, error, state FROM task WHERE "group" = '18' AND plan > :ct::timestamp;
INSERT INTO task ("group", input, count, save) VALUES ('19', 'CREATE TEMP TABLE save_probe_19 AS SELECT 1 AS a', 5, true);
INSERT INTO task ("group", input, count, save) VALUES ('19', 'SELECT count(*) FROM save_probe_19', 5, true);
INSERT INTO task ("group", input, count, save) VALUES ('20', 'CREATE TEMP TABLE save_probe_20 AS SELECT 1 AS a', 5, false);
INSERT INTO task ("group", input, count, save) VALUES ('20', 'SELECT count(*) FROM save_probe_20', 5, false);
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
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
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
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
SET pg_task.header = false;
SET pg_task.string = false;
SET pg_task.delimiter = ',';
SET pg_task."null" = '<NULL>';
SET pg_task.quote = '"';
DELETE FROM task WHERE "group" = '29';
INSERT INTO task ("group", input) VALUES ('29', 'SELECT 1 AS a, ''text'' AS b, NULL::int AS c');
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
INSERT INTO task ("group", input) VALUES ('30', 'SELECT pg_sleep(5) AS a');
INSERT INTO task ("group", input) VALUES ('31', 'SELECT pg_sleep(5) AS a');
INSERT INTO task ("group", input) VALUES ('32', 'SELECT pg_sleep(5) AS a');
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FILTER (WHERE state != 'PLAN') FROM task WHERE "group" IN ('30', '31', '32')) >= 1 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
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
INSERT INTO task ("group", input) VALUES ('30', 'SELECT pg_sleep(5) AS a');
INSERT INTO task ("group", input) VALUES ('31', 'SELECT pg_sleep(5) AS a');
INSERT INTO task ("group", input) VALUES ('32', 'SELECT pg_sleep(5) AS a');
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FILTER (WHERE state != 'PLAN') FROM task WHERE "group" IN ('30', '31', '32')) >= 1 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
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
INSERT INTO task ("group", input) VALUES ('33', 'BEGIN; SELECT 1 AS a');
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '33' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT output, error, state FROM task WHERE "group" = '33' AND plan > :ct::timestamp;
DELETE FROM task WHERE "group" = '34';
INSERT INTO task ("group", input) VALUES ('34', $task$DO $inner$ BEGIN RAISE EXCEPTION 'boom' USING DETAIL = 'detail text', HINT = 'hint text'; END $inner$;$task$);
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '34' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT output, error, state FROM task WHERE "group" = '34' AND plan > :ct::timestamp;
DELETE FROM task WHERE "group" = '35';
WITH s AS (SELECT generate_series(1, 16) AS s) INSERT INTO task ("group", input, live, timeout) SELECT '35', 'SELECT pg_sleep(0.3) AS a', '5 sec', '10 sec' FROM s;
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '35' AND state = 'DONE') >= 1 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM SET statement_timeout = '150ms';
SELECT pg_reload_conf();
SET statement_timeout = 0;
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '35' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM RESET statement_timeout;
SELECT pg_reload_conf();
RESET statement_timeout;
SELECT EXISTS (
    SELECT 1 FROM task a JOIN task b ON a.pid = b.pid
    WHERE a."group" = '35' AND b."group" = '35' AND a.plan > :ct::timestamp AND b.plan > :ct::timestamp
    AND a.state = 'DONE' AND b.state = 'FAIL' AND b.error LIKE '%statement timeout%'
) AS reused_worker_saw_reload;
CREATE TABLE deadlock_probe (id int PRIMARY KEY, val int);
INSERT INTO deadlock_probe VALUES (1, 0), (2, 0);
ALTER SYSTEM SET deadlock_timeout = '100ms';
SELECT pg_reload_conf();
DELETE FROM task WHERE "group" IN ('36', '37');
INSERT INTO task ("group", input) VALUES ('36', 'UPDATE deadlock_probe SET val = 1 WHERE id = 1; SELECT pg_sleep(2); UPDATE deadlock_probe SET val = 1 WHERE id = 2');
INSERT INTO task ("group", input) VALUES ('37', 'UPDATE deadlock_probe SET val = 1 WHERE id = 2; SELECT pg_sleep(2); UPDATE deadlock_probe SET val = 1 WHERE id = 1');
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" IN ('36', '37') AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) FILTER (WHERE state = 'FAIL' AND error LIKE '%deadlock detected%' AND error ~ 'Process \d+: ') = 1 AS deadlock_log_detail_ok, count(*) FILTER (WHERE state = 'DONE') = 1 AS other_committed FROM task WHERE "group" IN ('36', '37') AND plan > :ct::timestamp;
ALTER SYSTEM RESET deadlock_timeout;
SELECT pg_reload_conf();
DROP TABLE deadlock_probe;
ALTER SYSTEM SET log_error_verbosity = 'verbose';
SELECT pg_reload_conf();
SET check_function_bodies = off;
CREATE FUNCTION local_query_location_probe() RETURNS int LANGUAGE SQL AS $probe$SELECT SELEKT 1$probe$;
RESET check_function_bodies;
DELETE FROM task WHERE "group" = '38';
INSERT INTO task ("group", input) VALUES ('38', 'SELECT local_query_location_probe()');
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '38' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT error LIKE '%QUERY:  SELECT SELEKT 1%' AS query_field_ok, error LIKE '%LOCATION:  %, %:%' AS location_field_ok, state FROM task WHERE "group" = '38' AND plan > :ct::timestamp;
DROP FUNCTION local_query_location_probe();
ALTER SYSTEM RESET log_error_verbosity;
SELECT pg_reload_conf();
SET client_min_messages = warning;
CREATE ROLE task_owner_test LOGIN;
CREATE ROLE task_owner_test_b LOGIN;
RESET client_min_messages;
SELECT (SELECT count(*) FROM pg_catalog.pg_settings WHERE name = 'gp_role') > 0 AS is_gp
\gset
SELECT '/tmp/pg_task_gp_policy_' || pg_backend_pid() || '.sql' AS gp_policy_file
\gset
\o :gp_policy_file
SELECT CASE WHEN :'is_gp' = 't' THEN 'SELECT NOT EXISTS (SELECT 1 FROM gp_dist_random(' || chr(39) || 'pg_class' || chr(39) || ') WHERE oid = ' || chr(39) || 'task' || chr(39) || '::regclass) AS need_gp_utility' ELSE 'SELECT false AS need_gp_utility' END;
SELECT '\gset';
\o
\i :gp_policy_file
SELECT '/tmp/pg_task_gp_utility_' || pg_backend_pid() || '.sql' AS gp_utility_file
\gset
\o :gp_utility_file
SELECT CASE WHEN :'need_gp_utility' = 't' THEN '\connect "dbname=' || :'DBNAME' || ' options=' || chr(39) || '-c gp_session_role=utility' || chr(39) || '"' ELSE '' END;
\o
\i :gp_utility_file
GRANT INSERT ON task TO task_owner_test;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE task_id_seq TO task_owner_test;
GRANT INSERT ON task TO task_owner_test_b;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE task_id_seq TO task_owner_test_b;
\connect :DBNAME
DELETE FROM task WHERE "group" IN ('39', '40', '41', '42', '43');
SET ROLE task_owner_test;
INSERT INTO task ("group", input, header) VALUES ('39', 'SELECT NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) AS a', false);
RESET ROLE;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '39' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "user" = 'task_owner_test' AS owner_recorded, output = 't' AS ran_unprivileged, state FROM task WHERE "group" = '39' AND plan > :ct::timestamp;
SET ROLE task_owner_test;
INSERT INTO task ("group", input, "user") VALUES ('40', 'SELECT 1 AS a', 'postgres');
RESET ROLE;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '40' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "user" = 'task_owner_test' AS forged_owner_rejected, state FROM task WHERE "group" = '40' AND plan > :ct::timestamp;
INSERT INTO task ("group", input) VALUES ('41', 'SELECT 1 AS a');
DO $$ BEGIN
    UPDATE task SET "user" = 'task_owner_test' WHERE "group" = '41';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM <> 'user column is immutable' THEN RAISE; END IF;
END $$;
SELECT "user" = current_user AS owner_immutable FROM task WHERE "group" = '41' AND plan > :ct::timestamp;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '41' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SET ROLE task_owner_test;
INSERT INTO task ("group", input, repeat) VALUES ('42', 'SELECT pg_sleep(0.2) AS a', '1 sec');
RESET ROLE;
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FILTER (WHERE state = 'DONE') FROM task WHERE "group" = '42') >= 3 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) >= 3 AS repeated_enough, bool_and("user" = 'task_owner_test') AS owner_preserved_across_repeats FROM task WHERE "group" = '42' AND plan > :ct::timestamp;
DELETE FROM task WHERE "group" = '42';
SET ROLE task_owner_test;
INSERT INTO task ("group", input, header) VALUES ('43', 'SELECT current_user OPERATOR(pg_catalog.=) ''task_owner_test'' AS a', false);
RESET ROLE;
SET ROLE task_owner_test_b;
INSERT INTO task ("group", input, header) VALUES ('43', 'SELECT current_user OPERATOR(pg_catalog.=) ''task_owner_test_b'' AS a', false);
RESET ROLE;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = '43' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT bool_and(output = 't') AS each_task_saw_its_own_identity, count(DISTINCT "user") = 2 AS two_distinct_owners FROM task WHERE "group" = '43' AND plan > :ct::timestamp;
DELETE FROM task WHERE "group" IN ('39', '40', '41', '42', '43');
\i :gp_utility_file
REVOKE INSERT ON task FROM task_owner_test, task_owner_test_b;
REVOKE USAGE, SELECT, UPDATE ON SEQUENCE task_id_seq FROM task_owner_test, task_owner_test_b;
\connect :DBNAME
DROP ROLE task_owner_test;
DROP ROLE task_owner_test_b;
DELETE FROM task WHERE plan > :ct::timestamp; -- catch-all: remove anything this run inserted that an earlier per-group DELETE missed
