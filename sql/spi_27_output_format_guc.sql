SET pg_task.header = false;
SET pg_task.string = false;
SET pg_task.delimiter = ',';
SET pg_task."null" = '<NULL>';
SET pg_task.quote = '"';
DELETE FROM task WHERE "group" = 'output_format_guc';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input) VALUES ('output_format_guc', 'SELECT 1 AS a, ''text'' AS b, NULL::int AS c');
RESET pg_task.header;
RESET pg_task.string;
RESET pg_task.delimiter;
RESET pg_task."null";
RESET pg_task.quote;
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'output_format_guc' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT output = '"1","text",<NULL>' AS format_ok FROM task WHERE "group" = 'output_format_guc' AND plan > :ct::timestamp;
