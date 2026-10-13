-- MERGE reports the rows it merged in every mode, as MERGE n (spi mode used to leave the count out); before 15 there is no MERGE
DELETE FROM task WHERE "group" = 'merge';
SET client_min_messages = warning;
DROP TABLE IF EXISTS mg_m;
RESET client_min_messages;
CREATE TABLE mg_m (id int PRIMARY KEY, v int);
INSERT INTO mg_m VALUES (1, 1);
SELECT quote_literal('dbname=' || :'DBNAME') AS remote
\gset
INSERT INTO task ("group", "delete", remote, input) VALUES ('merge', false, :remote, 'MERGE INTO mg_m USING (VALUES (1), (2)) AS s(id) ON mg_m.id = s.id WHEN MATCHED THEN UPDATE SET v = 2 WHEN NOT MATCHED THEN INSERT VALUES (s.id, 3)');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'merge' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''merge'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, output, error IS NOT NULL AS failed FROM task WHERE "group" = 'merge';
DELETE FROM task WHERE "group" = 'merge';
DROP TABLE mg_m;
