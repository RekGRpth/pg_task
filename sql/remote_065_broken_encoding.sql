-- each completed statement of a remote task is converted to this database's encoding before the next one: even when its connection breaks, their output is kept, with the connection's error following it
DELETE FROM task WHERE "group" = 'broken_encoding';
INSERT INTO task ("group", input, remote) VALUES ('broken_encoding', 'SET client_encoding = ''LATIN1''; SELECT ' || quote_literal(chr(233)) || '; SELECT pg_terminate_backend(pg_backend_pid())', 'dbname=' || :'DBNAME');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'broken_encoding' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''broken_encoding'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pg_catalog.getdatabaseencoding() <> 'UTF8' OR (state = 'FAIL' AND output IN ('SET' || chr(10) || chr(233), 'SET' || chr(10) || chr(233) || chr(10) || 't') AND error LIKE '%terminating connection due to administrator command%' AND convert_to(error, 'UTF8') IS NOT NULL) AS ok FROM task WHERE "group" = 'broken_encoding';
DELETE FROM task WHERE "group" = 'broken_encoding';
