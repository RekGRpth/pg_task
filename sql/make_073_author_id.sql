-- a task author inserts a task of an id its sequence gave out already, the default one, or one of its own not past the last value of the sequence, a negative one or 0 say, not one past it, which would be taken ahead of the others, in STOP, never to run, hidden from them, their inserts and the repeats of their tasks failing on its key then, till the sequence got past it; the owner of the table, pg_task.user, and its members may give any
SET client_min_messages = warning;
DROP ROLE IF EXISTS task_author_id;
CREATE ROLE task_author_id;
RESET client_min_messages;
GRANT SELECT, INSERT, DELETE ON task TO task_author_id;
GRANT USAGE ON SEQUENCE task_id_seq TO task_author_id;
DELETE FROM task WHERE "group" = 'author_id';
SET ROLE task_author_id;
INSERT INTO task ("group", input, plan) VALUES ('author_id', 'SELECT 1', 'infinity') RETURNING id > 0 AS default_id
\gset
SELECT :'default_id' AS default_id;
SELECT set_config('pg_task_test.last_id', (SELECT max(id) FROM task WHERE "group" = 'author_id')::text, false) IS NOT NULL AS last_id_saved;
\set VERBOSITY terse
INSERT INTO task (id, "group", input, state) SELECT current_setting('pg_task_test.last_id')::int8 + 10, 'author_id', 'SELECT 1', 'STOP';
\set VERBOSITY default
INSERT INTO task (id, "group", input, plan) VALUES (-731731, 'author_id', 'SELECT 1', 'infinity');
RESET ROLE;
INSERT INTO task (id, "group", input, plan) SELECT current_setting('pg_task_test.last_id')::int8 + 1000000, 'author_id', 'SELECT 1', 'infinity';
SELECT count(*) AS inserted, count(*) FILTER (WHERE id < 0) AS negative FROM task WHERE "group" = 'author_id';
DELETE FROM task WHERE "group" = 'author_id';
DROP OWNED BY task_author_id;
DROP ROLE task_author_id;
