-- an entry is known by the names its schema and table get, cut to 63 bytes as identifiers are: two whose tables differ only past that are one, with one pg_work of their one table, not two
SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT repeat('l', 63) AS long_table
\gset
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"' || current_user || '","schema":"long_name","table":"' || :'long_table' || 'x"},{"data":"' || :'DBNAME' || '","user":"' || current_user || '","schema":"long_name","table":"' || :'long_table' || 'y"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'long_name' AND c.relname = repeat('l', 63)) AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work long_name %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker and table of long_name.%', repeat('l', 63); END IF;
END;$body$ LANGUAGE plpgsql;
-- the most pg_work of the table seen over a second, both of the entries started together if they were two
DO $body$ DECLARE most bigint := 0; BEGIN
    FOR i IN 1..10 LOOP
        PERFORM pg_stat_clear_snapshot();
        most := GREATEST(most, (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work long_name %'));
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF most <> 1 THEN RAISE EXCEPTION '% pg_work workers of long_name.%, not one', most, repeat('l', 63); END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work long_name %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of long_name.% to stop', repeat('l', 63); END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA IF EXISTS long_name CASCADE;
RESET client_min_messages;
