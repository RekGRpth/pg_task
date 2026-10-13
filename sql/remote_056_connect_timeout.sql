-- connecting to a remote server that doesn't answer gives up after the connect_timeout of the connection string, as a synchronous connection would, rather than keeping the task in TAKE for as long as the TCP timeout (192.0.2.1 is reserved for documentation, nothing answers it; where it's rejected at once instead, the task fails all the same)
DELETE FROM task WHERE "group" = 'connect_timeout';
INSERT INTO task ("group", input, remote) VALUES ('connect_timeout', 'SELECT 1 AS a', 'host=192.0.2.1 port=5432 dbname=postgres connect_timeout=2 password=x');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'connect_timeout' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''connect_timeout'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, error IS NOT NULL AS failed FROM task WHERE "group" = 'connect_timeout';
DELETE FROM task WHERE "group" = 'connect_timeout';
