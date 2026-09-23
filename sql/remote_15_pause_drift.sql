DELETE FROM task WHERE "group" = 'pause_drift';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
SELECT quote_literal(clock_timestamp()) AS ct14 \gset
WITH s AS (SELECT generate_series(1, 3) AS s) INSERT INTO task ("group", input, max, drift, remote) SELECT 'pause_drift', 'SELECT clock_timestamp() AS a', -1000, true, 'dbname=' || :'DBNAME' FROM s;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'pause_drift' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''pause_drift'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", min(start) - :ct14::timestamptz < interval '2500 ms' AS first_run_immediate, bool_and(gap IS NULL OR gap >= interval '1 sec') AS pause_ok FROM (
    SELECT "group", start, plan - lag(plan) OVER (ORDER BY plan) AS gap FROM task WHERE "group" = 'pause_drift' AND plan > :ct::timestamp
) x GROUP BY "group";
