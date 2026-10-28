-- spi mode tells CREATE TABLE IF NOT EXISTS ... AS of a table that exists (9.5+) by the schema the table would be created in, as PostgreSQL does, not by a table of the same name elsewhere on the search_path, a temporary one or one of a later schema, reporting the rows stored as local mode and remote mode do; with no schema to create in, its error is that of PostgreSQL, as without IF NOT EXISTS; the tasks of the group run one at a time, by id, -733 before -732
DELETE FROM task WHERE "group" = 'ctas_creation_schema';
SET client_min_messages = warning;
DROP SCHEMA IF EXISTS ctas_cs_a, ctas_cs_b CASCADE;
DROP TABLE IF EXISTS ctas_cs_t;
RESET client_min_messages;
CREATE SCHEMA ctas_cs_a;
CREATE SCHEMA ctas_cs_b;
CREATE TABLE ctas_cs_b.ctas_cs_u (i int);
INSERT INTO task (id, "group", input) VALUES
    (-731, 'ctas_creation_schema', 'CREATE TEMP TABLE ctas_cs_t AS SELECT 1 AS i; CREATE TABLE IF NOT EXISTS ctas_cs_t AS SELECT 2 AS i'),
    (-732, 'ctas_creation_schema', 'SET search_path = ctas_cs_a, ctas_cs_b; CREATE TABLE IF NOT EXISTS ctas_cs_u AS SELECT 4 AS i'),
    (-733, 'ctas_creation_schema', 'SET search_path = ctas_cs_a, ctas_cs_b; CREATE TABLE IF NOT EXISTS ctas_cs_u AS SELECT 3 AS i'),
    (-734, 'ctas_creation_schema', 'SET search_path = ''''; CREATE TABLE IF NOT EXISTS ctas_cs_v AS SELECT 5 AS i'),
    (-735, 'ctas_creation_schema', 'SET search_path = ''''; CREATE TABLE ctas_cs_v AS SELECT 5 AS i');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'ctas_creation_schema' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of group ctas_creation_schema to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
-- before 9.5 no IF NOT EXISTS of CREATE TABLE AS, each of them failing on its syntax
SELECT -id AS n, current_setting('server_version_num')::int < 90500 OR CASE id
    WHEN -731 THEN state = 'DONE' AND output = E'SELECT 1\nSELECT 1'
    WHEN -732 THEN state = 'DONE' AND output = E'SET\nCREATE TABLE AS'
    WHEN -733 THEN state = 'DONE' AND output = E'SET\nSELECT 1'
    ELSE state = 'FAIL' AND output = 'SET' AND error LIKE 'ERROR:  no schema has been selected to create in%'
END AS ok FROM task WHERE "group" = 'ctas_creation_schema' ORDER BY id DESC;
SELECT current_setting('server_version_num')::int < 90500 OR (SELECT error FROM task WHERE id = -734) = replace((SELECT error FROM task WHERE id = -735), 'CREATE TABLE ctas_cs_v', 'CREATE TABLE IF NOT EXISTS ctas_cs_v') AS same_error;
SELECT current_setting('server_version_num')::int < 90500 OR (to_regclass('ctas_cs_t') IS NOT NULL AND to_regclass('ctas_cs_a.ctas_cs_u') IS NOT NULL) AS created;
DELETE FROM task WHERE "group" = 'ctas_creation_schema';
SET client_min_messages = warning;
DROP SCHEMA ctas_cs_a, ctas_cs_b CASCADE;
DROP TABLE IF EXISTS ctas_cs_t;
RESET client_min_messages;
