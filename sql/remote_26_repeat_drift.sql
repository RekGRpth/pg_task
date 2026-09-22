DELETE FROM task WHERE "group" = 'repeat_drift';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, repeat, drift, remote) VALUES ('repeat_drift', 'SELECT pg_sleep(1) AS a', '3 sec', true, 'dbname=' || :'DBNAME');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..90 LOOP
        IF (SELECT count(*) FILTER (WHERE state = 'DONE') >= 3 FROM task WHERE "group" = 'repeat_drift') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 90 x pg_sleep(1) waiting for at least 3 DONE runs in repeating task group ''repeat_drift'''; END IF;
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" = 'repeat_drift' AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" = 'repeat_drift' AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" = 'repeat_drift' AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" = 'repeat_drift' AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" = 'repeat_drift' AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" = 'repeat_drift' AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" = 'repeat_drift' AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" = 'repeat_drift' AND state = 'PLAN';
DO $$ BEGIN PERFORM pg_sleep(1); END $$;
DELETE FROM task WHERE "group" = 'repeat_drift' AND state = 'PLAN';
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'repeat_drift' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 30 x pg_sleep(1) waiting for task group ''repeat_drift'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" = 'repeat_drift' AND state NOT IN ('DONE', 'GONE', 'FAIL');
WITH g AS (
    SELECT id, parent, lag(id) OVER (ORDER BY plan) AS prev_id, plan - lag(stop) OVER (ORDER BY plan) AS gap
    FROM task WHERE "group" = 'repeat_drift' AND plan > :ct::timestamp
)
SELECT count(*) >= 3 AS repeated_enough,
    bool_and(parent IS NOT DISTINCT FROM prev_id) AS parent_chain_ok,
    bool_and(gap IS NULL OR gap BETWEEN interval '2900 ms' AND interval '3500 ms') AS drift_from_stop_ok
FROM g;
