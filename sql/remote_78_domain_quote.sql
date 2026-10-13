-- a domain over a number or a boolean is written as one, unquoted with string, alike in local mode, spi mode and remote mode, where the server sends a client its base type; one over text is quoted as text
SET client_min_messages = warning;
DROP DOMAIN IF EXISTS domain_quote_int;
DROP DOMAIN IF EXISTS domain_quote_text;
RESET client_min_messages;
CREATE DOMAIN domain_quote_int AS int;
CREATE DOMAIN domain_quote_text AS text;
DELETE FROM task WHERE "group" = 'domain_quote';
INSERT INTO task ("group", input, quote, string, header, remote) SELECT 'domain_quote', $task$SELECT 5::domain_quote_int AS i, 'x'::domain_quote_text AS t, true AS b$task$, '"', true, false, 'dbname=' || :'DBNAME';
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'domain_quote' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''domain_quote'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, output, error FROM task WHERE "group" = 'domain_quote';
DELETE FROM task WHERE "group" = 'domain_quote';
DROP DOMAIN domain_quote_int;
DROP DOMAIN domain_quote_text;
