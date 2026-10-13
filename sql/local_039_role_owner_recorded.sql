DELETE FROM task WHERE "group" = 'role_owner_recorded';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
SET ROLE task_owner_test;
INSERT INTO task ("group", input, header) VALUES ('role_owner_recorded', 'SELECT NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) AS a', false);
RESET ROLE;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'role_owner_recorded' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''role_owner_recorded'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "user" = 'task_owner_test' AS owner_recorded, output = 't' AS ran_unprivileged, state FROM task WHERE "group" = 'role_owner_recorded' AND plan > :ct::timestamp;
