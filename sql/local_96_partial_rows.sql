-- the rows a statement returned before it failed are kept in the output, alike in local mode and spi mode, where they came in a few at a time and those of the fetch that failed were lost
DELETE FROM task WHERE "group" = 'partial_rows';
INSERT INTO task ("group", input) VALUES ('partial_rows', 'SELECT 1 / (3 - i) AS v FROM generate_series(1, 5) AS i');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'partial_rows' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''partial_rows'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, output, split_part(error, E'\n', 1) AS error FROM task WHERE "group" = 'partial_rows';
DELETE FROM task WHERE "group" = 'partial_rows';
