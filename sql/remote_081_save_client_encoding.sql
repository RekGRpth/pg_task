-- a remote task that keeps its session for the next one of its group, of save, but with a client_encoding its input set, has the next one run on a connection of its own, in the encoding of this database, rather than on that session, its input read in that client_encoding, its text taken amiss with no error (in a database not in UTF8 there is nothing to tell)
DELETE FROM task WHERE "group" = 'save_client_encoding';
INSERT INTO task ("group", remote, input, save, count) VALUES
    ('save_client_encoding', 'dbname=' || :'DBNAME', 'SET client_encoding = ''LATIN1''; SELECT 1 AS a', true, 2),
    ('save_client_encoding', 'dbname=' || :'DBNAME', 'SELECT pg_catalog.length(' || pg_catalog.quote_literal(chr(233)) || ') AS a', true, 2);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'save_client_encoding' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of group save_client_encoding to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pg_catalog.getdatabaseencoding() <> 'UTF8' OR (pg_catalog.array_agg(state::text ORDER BY id) = '{DONE,DONE}' AND (pg_catalog.array_agg(output ORDER BY id))[2] = '1' AND count(DISTINCT pid) = 2) AS ok FROM task WHERE "group" = 'save_client_encoding';
DELETE FROM task WHERE "group" = 'save_client_encoding';
