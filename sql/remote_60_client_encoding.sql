-- a remote task's results go into its text columns in the encoding of this database: a client_encoding in the connection string or in its options doesn't change that, and the results of an input setting one itself, stored only if they are text of this database, fail it here, instead of their bytes being stored as they came (in a database not in UTF8 there is nothing to tell)
DELETE FROM task WHERE "group" LIKE 'client_encoding_%';
INSERT INTO task ("group", remote, input) VALUES
    ('client_encoding_conninfo', 'dbname=' || :'DBNAME' || ' client_encoding=LATIN1', 'SELECT chr(252) AS a'),
    ('client_encoding_options', 'dbname=' || :'DBNAME' || ' options=''-c client_encoding=LATIN1''', 'SELECT chr(252) AS a'),
    ('client_encoding_input', 'dbname=' || :'DBNAME', 'SET client_encoding = ''LATIN1''; SELECT chr(252) AS a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" LIKE 'client_encoding_%' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task groups ''client_encoding_%%'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", pg_catalog.getdatabaseencoding() <> 'UTF8' OR CASE "group" WHEN 'client_encoding_input' THEN state = 'FAIL' AND error LIKE 'ERROR:  invalid byte sequence for encoding "UTF8": 0xfc%' ELSE state = 'DONE' AND output = chr(252) END AS ok FROM task WHERE "group" LIKE 'client_encoding_%' ORDER BY "group";
DELETE FROM task WHERE "group" LIKE 'client_encoding_%';
