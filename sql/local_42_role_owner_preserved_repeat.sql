DELETE FROM task WHERE "group" = 'role_owner_preserved_repeat';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
SET ROLE task_owner_test;
INSERT INTO task ("group", input, repeat) VALUES ('role_owner_preserved_repeat', 'SELECT pg_sleep(0.2) AS a', '1 sec');
RESET ROLE;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FILTER (WHERE state = 'DONE') FROM task WHERE "group" = 'role_owner_preserved_repeat') >= 3 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 30 x pg_sleep(1) waiting for at least 3 DONE runs in repeating task group ''role_owner_preserved_repeat'''; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) >= 3 AS repeated_enough, bool_and("user" = 'task_owner_test') AS owner_preserved_across_repeats FROM task WHERE "group" = 'role_owner_preserved_repeat' AND plan > :ct::timestamp;
DELETE FROM task WHERE "group" = 'role_owner_preserved_repeat';
