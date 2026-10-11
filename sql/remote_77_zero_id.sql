-- a remote task of id 0, which an author may insert, runs as any other, rather than be taken for one stopped, with no stop set, see work_input()
DELETE FROM task WHERE "group" = 'zero_id';
INSERT INTO task (id, "group", input, remote) VALUES (0, 'zero_id', 'SELECT 1 AS a', 'dbname=' || :'DBNAME');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'zero_id' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of group zero_id to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT id, state, output, error FROM task WHERE "group" = 'zero_id';
DELETE FROM task WHERE "group" = 'zero_id';
