DELETE FROM task WHERE "group" = 'basic_timeout';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, timeout, remote) VALUES ('basic_timeout', 'SELECT pg_sleep(2) AS a', '1 sec', 'dbname=' || :'DBNAME');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'basic_timeout' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = 'basic_timeout' AND plan > :ct::timestamp;
