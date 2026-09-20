DELETE FROM task WHERE "group" = 'role_two_distinct_owners';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
SET ROLE task_owner_test;
INSERT INTO task ("group", input, header) VALUES ('role_two_distinct_owners', 'SELECT current_user OPERATOR(pg_catalog.=) ''task_owner_test'' AS a', false);
RESET ROLE;
SET ROLE task_owner_test_b;
INSERT INTO task ("group", input, header) VALUES ('role_two_distinct_owners', 'SELECT current_user OPERATOR(pg_catalog.=) ''task_owner_test_b'' AS a', false);
RESET ROLE;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'role_two_distinct_owners' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT bool_and(output = 't') AS each_task_saw_its_own_identity, count(DISTINCT "user") = 2 AS two_distinct_owners FROM task WHERE "group" = 'role_two_distinct_owners' AND plan > :ct::timestamp;
