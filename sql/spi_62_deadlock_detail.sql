-- the error of a task has the detail the client gets, not the one for the server log only, which has the queries of the other sessions of a deadlock in it
DELETE FROM task WHERE "group" LIKE 'deadlock_detail_%';
CREATE TABLE deadlock_detail_a (i int);
CREATE TABLE deadlock_detail_b (i int);
GRANT ALL ON deadlock_detail_a, deadlock_detail_b TO PUBLIC;
INSERT INTO task ("group", input) VALUES
    ('deadlock_detail_1', 'LOCK TABLE deadlock_detail_a; SELECT pg_sleep(1); LOCK TABLE deadlock_detail_b'),
    ('deadlock_detail_2', 'LOCK TABLE deadlock_detail_b; SELECT pg_sleep(1); LOCK TABLE deadlock_detail_a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" LIKE 'deadlock_detail_%' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task groups ''deadlock_detail_%%'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) FILTER (WHERE state = 'FAIL' AND error LIKE '%deadlock detected%') AS deadlocked, count(*) FILTER (WHERE error ~ 'Process [0-9]+: ') AS queries_shown FROM task WHERE "group" LIKE 'deadlock_detail_%';
DELETE FROM task WHERE "group" LIKE 'deadlock_detail_%';
DROP TABLE deadlock_detail_a, deadlock_detail_b;
