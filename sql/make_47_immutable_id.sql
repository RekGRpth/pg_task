-- the id of a task is immutable, as its group, remote and parent are: the lock of a running task is by its id, which a change of would leave it for work_reset() to run again, and its bookkeeping without the row
DELETE FROM task WHERE "group" = 'immutable_id';
INSERT INTO task ("group", plan, input) VALUES ('immutable_id', now() + interval '1 hour', 'SELECT 1');
\set VERBOSITY terse
UPDATE task SET id = id + 1000000000 WHERE "group" = 'immutable_id';
\set VERBOSITY default
SELECT count(*) AS tasks FROM task WHERE "group" = 'immutable_id';
DELETE FROM task WHERE "group" = 'immutable_id';
