-- before 12 an INSERT of one row into a table WITH OIDS reports its oid, in spi mode too, as in local mode and remote mode, and 0 for several; from 12 on there are no oids to report
DO $body$ BEGIN
    IF current_setting('server_version_num')::int < 120000 THEN EXECUTE 'CREATE TABLE insert_oid (a int) WITH OIDS'; ELSE EXECUTE 'CREATE TABLE insert_oid (a int)'; END IF;
END;$body$ LANGUAGE plpgsql;
GRANT INSERT ON insert_oid TO PUBLIC;
DELETE FROM task WHERE "group" = 'insert_oid';
INSERT INTO task ("group", input) VALUES ('insert_oid', 'INSERT INTO insert_oid VALUES (1); INSERT INTO insert_oid VALUES (2), (3)');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'insert_oid' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''insert_oid'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, output ~ CASE WHEN current_setting('server_version_num')::int < 120000 THEN '^INSERT [1-9][0-9]* 1\nINSERT 0 2$' ELSE '^INSERT 0 1\nINSERT 0 2$' END AS output_ok, error FROM task WHERE "group" = 'insert_oid';
DELETE FROM task WHERE "group" = 'insert_oid';
DROP TABLE insert_oid;
