-- an input of several statements runs them one by one, each with its own result, before 10 too, where no stmt_location tells them apart: split at each ; outside quotes, dollar quotes, comments and parentheses, those of the actions of CREATE RULE say, an empty statement giving nothing
DELETE FROM task WHERE "group" LIKE 'split_%';
CREATE TABLE split_r (i int);
CREATE TABLE split_l (i int);
GRANT ALL ON split_r, split_l TO PUBLIC;
INSERT INTO task ("group", input) VALUES
    ('split_quotes', 'SELECT '';'' AS a; SELECT $x$;$x$ AS b; /* ; */ SELECT 3 -- ;
'),
    ('split_empty', 'SELECT 1;; ; SELECT 2;'),
    ('split_rule', 'CREATE RULE split_rr AS ON INSERT TO split_r DO ALSO (INSERT INTO split_l VALUES (1); INSERT INTO split_l VALUES (2)); INSERT INTO split_r VALUES (0); SELECT count(*) FROM split_l'),
    ('split_do', 'DO $$BEGIN PERFORM 1; PERFORM 2; END$$; SELECT 4'),
    ('split_error', 'SELECT 1 AS a, 2 AS b; SELECT 1/0');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" LIKE 'split_%' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task groups ''split_%%'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", state, replace(output, E'\n', '|') AS output, error LIKE 'ERROR:  division by zero%' AS its_error FROM task WHERE "group" LIKE 'split_%' ORDER BY id;
DELETE FROM task WHERE "group" LIKE 'split_%';
DROP TABLE split_r, split_l;
