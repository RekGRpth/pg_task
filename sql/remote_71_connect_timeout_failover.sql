-- with several hosts in the connection string, connect_timeout applies to each of them, as a synchronous connection has it: a host that doesn't answer (192.0.2.1 is reserved for documentation) is given up for the next one, this very server, rather than failing the task; libpq knows of no lists of hosts before 10
DELETE FROM task WHERE "group" = 'connect_timeout_failover';
INSERT INTO task ("group", input, remote) SELECT 'connect_timeout_failover', 'SELECT 1 AS a', 'host=192.0.2.1,' || split_part(current_setting('unix_socket_directories'), ',', 1) || ' port=5432,' || current_setting('port') || ' dbname=' || :'DBNAME' || ' connect_timeout=2' WHERE current_setting('server_version_num')::int >= 100000;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'connect_timeout_failover' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''connect_timeout_failover'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT bool_and(state = 'DONE' AND error IS NULL) IS NOT FALSE AS done FROM task WHERE "group" = 'connect_timeout_failover';
DELETE FROM task WHERE "group" = 'connect_timeout_failover';
