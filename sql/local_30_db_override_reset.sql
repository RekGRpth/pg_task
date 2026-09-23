DELETE FROM task WHERE "group" = 'db_override_reset';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input) VALUES ('db_override_reset', 'SELECT pg_sleep(10) AS a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..150 LOOP
        IF (SELECT state FROM task WHERE "group" = 'db_override_reset') = 'WORK' THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 150 x pg_sleep(0.1) waiting for task in group ''db_override_reset'' to reach WORK state'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pid AS orig_pid_db FROM task WHERE "group" = 'db_override_reset'
\gset
SELECT count(pg_terminate_backend(:orig_pid_db)) > 0 AS worker_killed_db_override;
ALTER DATABASE :DBNAME SET pg_task.reset = '2 sec';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT state FROM task WHERE "group" = 'db_override_reset') = 'DONE' THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task in group ''db_override_reset'' to reach DONE state'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER DATABASE :DBNAME RESET pg_task.reset;
SELECT pg_reload_conf();
SELECT state = 'DONE' AS recovered_via_db_override, pid != :orig_pid_db AS pid_changed_db_override FROM task WHERE "group" = 'db_override_reset';
