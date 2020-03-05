# pg_task config
to run pg_task add it to line
```conf
shared_preload_libraries = 'pg_task'
```

by default pg_task use table
```conf
pg_task.default_table = 'task'
```
and sleep timeout
```conf
pg_task.default_timeout = 1000
```
milliseconds
and reset count
```conf
pg_task.default_reset = 60
```
of timeout

by default pg_task run on local database `postgres` with user `postgres` with default schema (in search path) with default table (as abow) and default timeout (as abow)

to run specific database and/or user and/or schema and/or table and/or timeout set line (in json format)
```conf
pg_task.json = '[{"data":"database1"},{"data":"database2","user":"username2"},{"data":"database3","schema":"schema3"},{"data":"database4","table":"table4"},{"data":"database5","timeout":100}]'
```

if database and/or user and/or schema and/or table does not exist then pg_task create it

# pg_task using

by default pg_task create table with folowing columns

id bigserial - primary key
parent bigint - foreign key to parent task (if need)
dt timestamp - planned time of start
start timestamp - actial time of start
stop timestamp - actial time of stop
group text - groupping task
max int - maximum concurently tasks in group
pid int - id of process executing task
request text - sql to execute
response text - result received
state state - PLAN, TAKE, WORK, DONE, FAIL or STOP
timeout interval - allowed time to run
delete boolean - autodelete (if response is null)
repeat interval - autorepeat interval
drift boolean - see below
count integer - maximum task executed by current worker
live interval - maximum time of live of current worker
remote text - connect to remote database (if need)

but you may add any needed colums and/or make partitions

to run task more quickly execute sql command
```sql
INSERT INTO task (request) VALUES ('SELECT now()')
```

to run task after 5 minutes write plannded time
```sql
INSERT INTO task (dt, request) VALUES (now() + '5 min':INTERVAL, 'SELECT now()')
```

to run task at specific time so write
```sql
INSERT INTO task (dt, request) VALUES ('2029-07-01 12:51:00', 'SELECT now()')
```

to repeat task every 5 minutes write
```sql
INSERT INTO task (repeat, request) VALUES ('5 min', 'SELECT now()')
```

if write so
```sql
INSERT INTO task (repeat, request, drift) VALUES ('5 min', 'SELECT now()', false)
```
then repeat task will start after 5 minutes after task done (instead after planned time as default)

if exception occures it catched and writed in result as text
```sql
INSERT INTO task (request) VALUES ('SELECT 1/0')
```

if some group needs concurently run only 2 tasks then use command
```sql
INSERT INTO task (group, max, request) VALUES ('group', 2, 'SELECT now()')
```

if in this group there are more tasks and they are executing concurently by 2 then command
```sql
INSERT INTO task (group, max, request) VALUES ('group', 3, 'SELECT now()')
```
will execute task as more early in this group (as like priority)

to run task on remote database use sql command
```sql
INSERT INTO task (request, remote) VALUES ('SELECT now()', 'user=user host=host')
```
