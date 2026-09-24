DELETE FROM task WHERE "group" = 'spi_logging';
-- statement logging, with the parameters of pg_task's own queries, and then duration logging alone: nothing of it may break a task
ALTER SYSTEM SET log_statement = 'all';
ALTER SYSTEM SET log_min_duration_statement = 0;
SELECT pg_reload_conf();
SELECT pg_sleep(0.5);
INSERT INTO task ("group", input) VALUES ('spi_logging', 'SELECT 1 AS a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'spi_logging' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''spi_logging'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM RESET log_statement;
SELECT pg_reload_conf();
SELECT pg_sleep(0.5);
INSERT INTO task ("group", input) VALUES ('spi_logging', 'SELECT 2 AS a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'spi_logging' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''spi_logging'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM RESET log_min_duration_statement;
SELECT pg_reload_conf();
SELECT input, output, error, state FROM task WHERE "group" = 'spi_logging' ORDER BY id;
DELETE FROM task WHERE "group" = 'spi_logging';
