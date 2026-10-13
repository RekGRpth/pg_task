-- FETCH reports its rows only, none at all for no rows, and MOVE its count, alike in local mode and spi mode, where SPI tells neither
DELETE FROM task WHERE "group" = 'fetch_move_tags';
INSERT INTO task ("group", input) VALUES ('fetch_move_tags', 'DECLARE fetch_move_tags CURSOR WITH HOLD FOR SELECT i AS a FROM generate_series(1, 5) AS i; MOVE 2 IN fetch_move_tags; FETCH 2 FROM fetch_move_tags; FETCH 5 FROM fetch_move_tags; FETCH fetch_move_tags; MOVE ALL IN fetch_move_tags; CLOSE fetch_move_tags');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'fetch_move_tags' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''fetch_move_tags'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, output, error FROM task WHERE "group" = 'fetch_move_tags';
DELETE FROM task WHERE "group" = 'fetch_move_tags';
