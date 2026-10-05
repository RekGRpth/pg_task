-- a plan of infinity either way is refused, as the check of the other times is, which it passed, its sums infinite too: pg_work going idle, or the pause of a group with max < 0, subtract the time from it, failing outside any error handling, again and again
DELETE FROM task WHERE "group" = 'infinite_plan';
\set VERBOSITY terse
INSERT INTO task ("group", input, plan) VALUES ('infinite_plan', 'SELECT 1', 'infinity');
INSERT INTO task ("group", input, plan, max) VALUES ('infinite_plan', 'SELECT 1', '-infinity', -1000);
\set VERBOSITY default
SELECT count(*) AS inserted FROM task WHERE "group" = 'infinite_plan';
DELETE FROM task WHERE "group" = 'infinite_plan';
