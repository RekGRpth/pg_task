-- a task author inserts a task in PLAN, or in STOP, never to run, as a trigger of its own before insert may set it, as the state machine lets it change one to STOP only, with no start either way: one in STOP, with a start and no stop, inserted so, or in PLAN, set to STOP then, would be counted as running in its group for good; the owner of the table, pg_task.user, and its members may insert one in any state
SET client_min_messages = warning;
DROP ROLE IF EXISTS task_insert_state;
CREATE ROLE task_insert_state;
RESET client_min_messages;
GRANT SELECT, INSERT, DELETE ON task TO task_insert_state;
GRANT USAGE ON SEQUENCE task_id_seq TO task_insert_state;
DELETE FROM task WHERE "group" = 'insert_state';
SET ROLE task_insert_state;
\set VERBOSITY terse
INSERT INTO task ("group", input, plan, state, start) VALUES ('insert_state', 'SELECT 1', 'infinity', 'STOP', now());
INSERT INTO task ("group", input, plan, start) VALUES ('insert_state', 'SELECT 1', 'infinity', now());
INSERT INTO task ("group", input, plan, state) VALUES ('insert_state', 'SELECT 1', 'infinity', 'DONE');
\set VERBOSITY default
INSERT INTO task ("group", input, plan) VALUES ('insert_state', 'SELECT 1', 'infinity');
INSERT INTO task ("group", input, state) VALUES ('insert_state', 'SELECT 1', 'STOP');
SELECT state, start IS NULL AS no_start FROM task WHERE "group" = 'insert_state' ORDER BY id;
RESET ROLE;
INSERT INTO task ("group", input, plan, state) VALUES ('insert_state', 'SELECT 1', 'infinity', 'STOP');
SELECT state FROM task WHERE "group" = 'insert_state' ORDER BY id;
DELETE FROM task WHERE "group" = 'insert_state';
REVOKE ALL ON task FROM task_insert_state;
REVOKE ALL ON SEQUENCE task_id_seq FROM task_insert_state;
DROP ROLE task_insert_state;
