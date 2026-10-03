-- the pause of a negative max holds a task of the group planned within it too, not only one already due when the task before finishes: with drift as without, the second task, planned 1.5 s after the first, starts no sooner than 3 s after the first was planned
DELETE FROM task WHERE "group" IN ('pause_later_no_drift', 'pause_later_drift');
SELECT quote_literal('dbname=' || :'DBNAME') AS remote
\gset
INSERT INTO task ("group", remote, input, max, drift, plan) SELECT g, :remote, 'SELECT clock_timestamp() AS a', -3000, d, clock_timestamp() + o FROM (VALUES ('pause_later_no_drift', false), ('pause_later_drift', true)) AS v (g, d), (VALUES (interval '0 sec'), (interval '1.5 sec')) AS w (o);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" IN ('pause_later_no_drift', 'pause_later_drift') AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task groups ''pause_later_no_drift'' and ''pause_later_drift'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", count(*) AS tasks, bool_and(state = 'DONE') AS done, max(start) - min(plan) >= interval '2500 ms' AS paused FROM task WHERE "group" IN ('pause_later_no_drift', 'pause_later_drift') GROUP BY "group" ORDER BY "group";
DELETE FROM task WHERE "group" IN ('pause_later_no_drift', 'pause_later_drift');
