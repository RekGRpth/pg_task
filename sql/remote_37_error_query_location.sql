ALTER SYSTEM SET log_error_verbosity = 'verbose';
SELECT pg_reload_conf();
SET check_function_bodies = off;
CREATE FUNCTION query_location_probe() RETURNS int LANGUAGE SQL AS $probe$SELECT SELEKT 1$probe$;
RESET check_function_bodies;
DELETE FROM task WHERE "group" = 'error_query_location';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, remote) VALUES ('error_query_location', 'SELECT query_location_probe()', 'dbname=' || :'DBNAME');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'error_query_location' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 15 x pg_sleep(1) waiting for task group ''error_query_location'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT error LIKE '%QUERY:  SELECT SELEKT 1%' AS query_field_ok, error LIKE '%LOCATION:  %, %:%' AS location_field_ok, state FROM task WHERE "group" = 'error_query_location' AND plan > :ct::timestamp;
DROP FUNCTION query_location_probe();
ALTER SYSTEM RESET log_error_verbosity;
SELECT pg_reload_conf();
