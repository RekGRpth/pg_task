DELETE FROM task WHERE "group" IN ('limit_db_a', 'limit_db_b', 'limit_db_c');
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
ALTER DATABASE :DBNAME SET pg_task."limit" = 1;
SELECT pg_reload_conf();
INSERT INTO task ("group", input, remote) VALUES ('limit_db_a', 'SELECT pg_sleep(5) AS a', 'dbname=' || :'DBNAME');
INSERT INTO task ("group", input, remote) VALUES ('limit_db_b', 'SELECT pg_sleep(5) AS a', 'dbname=' || :'DBNAME');
INSERT INTO task ("group", input, remote) VALUES ('limit_db_c', 'SELECT pg_sleep(5) AS a', 'dbname=' || :'DBNAME');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FILTER (WHERE state != 'PLAN') FROM task WHERE "group" IN ('limit_db_a', 'limit_db_b', 'limit_db_c')) >= 1 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) FILTER (WHERE state != 'PLAN') >= 1 AS some_dispatched_limit_db_override, count(*) FILTER (WHERE state = 'PLAN') >= 1 AS some_capped_limit_db_override FROM task WHERE "group" IN ('limit_db_a', 'limit_db_b', 'limit_db_c') AND plan > :ct::timestamp;
ALTER DATABASE :DBNAME RESET pg_task."limit";
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" IN ('limit_db_a', 'limit_db_b', 'limit_db_c') AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" IN ('limit_db_a', 'limit_db_b', 'limit_db_c');
