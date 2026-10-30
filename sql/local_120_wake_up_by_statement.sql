-- the wake-up trigger fires for a statement, not for each row, and in Greengage asks no segment for the pg_work to wake, as its pg_locks does, with every row inserted
SELECT (t.tgtype & 1) = 0 AS by_statement, (p.prosrc LIKE '%pg_locks%') <> (version() ~ 'Greenplum|Greengage') AS asks_no_segment
FROM pg_trigger AS t JOIN pg_proc AS p ON p.oid = t.tgfoid WHERE t.tgrelid = 'task'::regclass AND t.tgname = 'task_wake_up';
