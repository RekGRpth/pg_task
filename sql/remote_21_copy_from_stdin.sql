SET client_min_messages = warning;
DROP TABLE IF EXISTS copy_probe;
CREATE TABLE copy_probe (a int);
RESET client_min_messages;
DELETE FROM task WHERE "group" = 'copy_from_stdin';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, remote) VALUES ('copy_from_stdin', 'COPY copy_probe FROM STDIN', 'dbname=' || :'DBNAME');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'copy_from_stdin' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = 'copy_from_stdin' AND plan > :ct::timestamp;
DROP TABLE copy_probe;
