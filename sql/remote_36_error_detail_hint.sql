DELETE FROM task WHERE "group" = 'error_detail_hint';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, remote) VALUES ('error_detail_hint', $task$DO $inner$ BEGIN RAISE EXCEPTION 'boom' USING DETAIL = 'detail text', HINT = 'hint text'; END $inner$;$task$, 'dbname=' || :'DBNAME');
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'error_detail_hint' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT output, error, state FROM task WHERE "group" = 'error_detail_hint' AND plan > :ct::timestamp;
