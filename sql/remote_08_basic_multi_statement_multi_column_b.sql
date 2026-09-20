DELETE FROM task WHERE "group" = 'basic_multi_stmt_multi_col_b';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, remote) VALUES ('basic_multi_stmt_multi_col_b', 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c, 4 AS d', 'dbname=' || :'DBNAME');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'basic_multi_stmt_multi_col_b' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = 'basic_multi_stmt_multi_col_b' AND plan > :ct::timestamp;
