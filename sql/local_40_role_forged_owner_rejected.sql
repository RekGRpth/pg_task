DELETE FROM task WHERE "group" = 'role_forged_owner_rejected';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
SET ROLE task_owner_test;
INSERT INTO task ("group", input, "user") VALUES ('role_forged_owner_rejected', 'SELECT 1 AS a', 'postgres');
RESET ROLE;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'role_forged_owner_rejected' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''role_forged_owner_rejected'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "user" = 'task_owner_test' AS forged_owner_rejected, state FROM task WHERE "group" = 'role_forged_owner_rejected' AND plan > :ct::timestamp;
