DELETE FROM task WHERE "group" = 'role_two_distinct_owners';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
SET ROLE task_owner_test;
INSERT INTO task ("group", input, header) VALUES ('role_two_distinct_owners', 'SELECT current_user OPERATOR(pg_catalog.=) ''task_owner_test'' AS a', false);
RESET ROLE;
SET ROLE task_owner_test_b;
INSERT INTO task ("group", input, header) VALUES ('role_two_distinct_owners', 'SELECT current_user OPERATOR(pg_catalog.=) ''task_owner_test_b'' AS a', false);
RESET ROLE;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'role_two_distinct_owners' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''role_two_distinct_owners'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT bool_and(output = 't') AS each_task_saw_its_own_identity, count(DISTINCT "user") = 2 AS two_distinct_owners FROM task WHERE "group" = 'role_two_distinct_owners' AND plan > :ct::timestamp;
-- row level security: a role neither sees nor changes the tasks of another one, so it can't rewrite someone else's queued input to have it run as them
SET ROLE task_owner_test_b;
INSERT INTO task ("group", plan, input) VALUES ('role_two_distinct_owners', now() + '1 hour', 'SELECT 1 AS a');
RESET ROLE;
SET ROLE task_owner_test;
SELECT count(*) AS visible, bool_and("user" = current_user) AS only_own_visible FROM task WHERE "group" = 'role_two_distinct_owners';
WITH u AS (UPDATE task SET input = 'SELECT 2 AS a' WHERE "group" = 'role_two_distinct_owners' AND state = 'PLAN' RETURNING 1) SELECT count(*) AS foreign_updated FROM u;
WITH d AS (DELETE FROM task WHERE "group" = 'role_two_distinct_owners' AND state = 'PLAN' RETURNING 1) SELECT count(*) AS foreign_deleted FROM d;
RESET ROLE;
SET ROLE task_owner_test_b;
WITH u AS (UPDATE task SET input = 'SELECT 3 AS a' WHERE "group" = 'role_two_distinct_owners' AND state = 'PLAN' RETURNING 1) SELECT count(*) AS own_updated FROM u;
RESET ROLE;
SELECT input FROM task WHERE "group" = 'role_two_distinct_owners' AND state = 'PLAN';
-- a member of the table owner may insert a task as another role, as the user trigger lets it, even when it doesn't inherit the owner's rights and so is bound by the policy
SET ROLE task_owner_test_c;
INSERT INTO task ("group", plan, input, "user") VALUES ('role_two_distinct_owners', now() + '1 hour', 'SELECT 1 AS a', 'task_owner_test_b') RETURNING "user";
RESET ROLE;
DELETE FROM task WHERE "group" = 'role_two_distinct_owners' AND state = 'PLAN';
