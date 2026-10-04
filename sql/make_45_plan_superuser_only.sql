-- pg_task.plan is an SQL expression, which the bookkeeping of a task, run as pg_task.user, and pg_work run as is: a task author may set it neither in its session nor for its own role, for its task worker to run what it likes as pg_task.user
SET client_min_messages = warning;
DROP ROLE IF EXISTS task_plan_author;
CREATE ROLE task_plan_author;
RESET client_min_messages;
SET ROLE task_plan_author;
SET pg_task.plan = 'statement_timestamp()';
ALTER ROLE task_plan_author SET pg_task.plan = 'statement_timestamp()';
RESET ROLE;
SELECT count(*) AS role_settings FROM pg_catalog.pg_db_role_setting WHERE setrole = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'task_plan_author');
DROP ROLE task_plan_author;
