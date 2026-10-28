-- pg_work.idle of the role of an entry in its database, ALTER ROLE ... IN DATABASE ... SET, takes effect on the reload, as the other settings of an entry do, rather than only once pg_work is started again: an idle pg_work, polling no more, polls again every sleep once its idle is raised so
SELECT current_user AS test_user
\gset
CREATE FUNCTION idle_role_polls() RETURNS int LANGUAGE plpgsql AS $function$DECLARE starts timestamptz[] := '{}'; s timestamptz; BEGIN
    FOR i IN 1..30 LOOP
        PERFORM pg_stat_clear_snapshot();
        SELECT query_start INTO s FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = current_database();
        IF s IS NOT NULL AND NOT s = ANY(starts) THEN starts := starts || s; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    RETURN pg_catalog.array_length(starts, 1);
END;$function$;
ALTER SYSTEM SET pg_task.sleep = 200;
ALTER SYSTEM SET pg_work.idle = 5;
SELECT pg_reload_conf();
SELECT pg_sleep(3);
SELECT current_setting('server_version_num')::int < 90500 OR idle_role_polls() < 8 AS idle;
ALTER ROLE :"test_user" IN DATABASE :"DBNAME" SET pg_work.idle = 1000000;
SELECT pg_reload_conf();
SELECT pg_sleep(1);
SELECT current_setting('server_version_num')::int < 90500 OR idle_role_polls() >= 10 AS polling; -- from 9.5 on, with pg_file_settings to tell the server's configuration files by, see init_settings()
ALTER ROLE :"test_user" IN DATABASE :"DBNAME" RESET pg_work.idle;
ALTER SYSTEM RESET pg_work.idle;
ALTER SYSTEM RESET pg_task.sleep;
SELECT pg_reload_conf();
DROP FUNCTION idle_role_polls();
