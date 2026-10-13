-- an entry is known by the hash of its schema and table, which a dot in a name mustn't make the same for two of them: schema hash_dot.x with table t and schema hash_dot with table x.t both get their pg_work and table
SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"' || current_user || '","schema":"hash_dot.x","table":"t"},{"data":"' || :'DBNAME' || '","user":"' || current_user || '","schema":"hash_dot","table":"x.t"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF (SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE (n.nspname, c.relname) IN (('hash_dot.x', 't'), ('hash_dot', 'x.t'))) = 2 AND (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work hash_dot.x t %' OR application_name LIKE 'pg_work hash_dot x.t %') = 2 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work workers and tables of both "hash_dot.x".t and hash_dot."x.t"'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work hash_dot.x t %' OR application_name LIKE 'pg_work hash_dot x.t %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work workers of "hash_dot.x".t and hash_dot."x.t" to stop'; END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA IF EXISTS "hash_dot.x" CASCADE;
DROP SCHEMA IF EXISTS hash_dot CASCADE;
RESET client_min_messages;
