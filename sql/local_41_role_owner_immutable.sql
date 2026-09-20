DELETE FROM task WHERE "group" = 'role_owner_immutable';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input) VALUES ('role_owner_immutable', 'SELECT 1 AS a');
DO $$ BEGIN
    UPDATE task SET "user" = 'task_owner_test' WHERE "group" = 'role_owner_immutable';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM <> 'user column is immutable' THEN RAISE; END IF;
END $$;
SELECT "user" = current_user AS owner_immutable FROM task WHERE "group" = 'role_owner_immutable' AND plan > :ct::timestamp;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'role_owner_immutable' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
