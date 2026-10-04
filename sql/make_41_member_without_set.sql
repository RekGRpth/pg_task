-- a role may act as the author of a task, insert it as them and see it, only if it may SET ROLE to them, as pg_task.user is checked with: from 16 on a member WITH SET FALSE may not, so its task is its own and the other's task out of its sight (before 16 every member may, so here it isn't one)
SET client_min_messages = warning;
DROP ROLE IF EXISTS task_set_target, task_set_member;
CREATE ROLE task_set_target;
CREATE ROLE task_set_member;
RESET client_min_messages;
DO $body$ BEGIN
    IF current_setting('server_version_num')::int >= 160000 THEN EXECUTE 'GRANT task_set_target TO task_set_member WITH SET FALSE, INHERIT FALSE'; END IF;
END;$body$ LANGUAGE plpgsql;
GRANT SELECT, INSERT ON task TO task_set_member;
SELECT 'GRANT USAGE ON SEQUENCE ' || pg_get_serial_sequence('task', 'id') || ' TO task_set_member' AS grant_sequence
\gset
:grant_sequence;
INSERT INTO task ("group", plan, input, "user") VALUES ('member_without_set_target', now() + interval '1 hour', 'SELECT 1', 'task_set_target');
SET ROLE task_set_member;
INSERT INTO task ("group", plan, input, "user") VALUES ('member_without_set', now() + interval '1 hour', 'SELECT 1', 'task_set_target');
SELECT count(*) = 1 OR current_setting('server_version_num')::int < 90500 AS only_own_visible FROM task WHERE "group" LIKE 'member_without_set%';
RESET ROLE;
SELECT "user" = 'task_set_member' AS inserted_as_itself FROM task WHERE "group" = 'member_without_set';
DELETE FROM task WHERE "group" LIKE 'member_without_set%';
REVOKE SELECT, INSERT ON task FROM task_set_member;
SELECT 'REVOKE USAGE ON SEQUENCE ' || pg_get_serial_sequence('task', 'id') || ' FROM task_set_member' AS revoke_sequence
\gset
:revoke_sequence;
DROP ROLE task_set_member;
DROP ROLE task_set_target;
