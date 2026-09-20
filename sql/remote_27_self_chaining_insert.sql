DELETE FROM task WHERE "group" = 'self_chaining_insert';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, remote) VALUES ('self_chaining_insert', 'INSERT INTO task ("group", input, remote) VALUES (''self_chaining_insert'', ''SELECT 1 AS a'', ''dbname=' || :'DBNAME' || ''')', 'dbname=' || :'DBNAME');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'self_chaining_insert') >= 2 AND (SELECT count(*) FROM task WHERE "group" = 'self_chaining_insert' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
WITH g AS (
    SELECT id, parent, input, state FROM task WHERE "group" = 'self_chaining_insert' AND plan > :ct::timestamp
)
SELECT count(*) = 2 AS both_rows_present,
    bool_and(state = 'DONE') AS both_done,
    (SELECT parent FROM g WHERE input LIKE 'SELECT%') = (SELECT id FROM g WHERE input LIKE 'INSERT%') AS chaining_ok
FROM g;
