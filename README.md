PostgreSQL and Greenplum job scheduler `pg_task` allows to execute any sql command at any specific time at background asynchronously

first
```conf
shared_preload_libraries = 'pg_task' # add pg_task to shared_preload_libraries
```
second
```sql
INSERT INTO task (input) VALUES ('SELECT now()'); -- to run sql more quickly use only input
INSERT INTO task (plan, input) VALUES (now() + '5 min':INTERVAL, 'SELECT now()'); -- to run sql after 5 minutes point plan(ned time)
INSERT INTO task (plan, input) VALUES ('2029-07-01 12:51:00', 'SELECT now()'); -- to run sql at specific time point it as plan(ned time)
INSERT INTO task (repeat, input) VALUES ('5 min', 'SELECT now()'); -- to repeat sql every 5 minutes point repeat( interval)
INSERT INTO task (input) VALUES ('SELECT 1/0'); -- exception is catched and writed in error as text
INSERT INTO task (group, max, input) VALUES ('group', 1, 'SELECT now()'); -- if some group needs concurently run only 2 parallel sqls then use max = 1
INSERT INTO task (group, max, input) VALUES ('group', 2, 'SELECT now()'); -- if in this group there are more sqls and they are executing concurently by 2 then passing max = 2 will execute sql as more early in this group (it is like priority)
INSERT INTO task (input, remote) VALUES ('SELECT now()', 'user=user host=host'); -- to run sql on remote database use remote
```

`pg_task` creates folowing GUCs

| Name | Type | Default | Level | Description |
| --- | --- | --- | --- | --- |
| pg_task.delete | bool | true | config, database, user, session | Auto delete task when both output and error are nulls |
| pg_task.drift | bool | false | config, database, user, session | Compute next repeat time by stop time instead by plan time |
| pg_task.header | bool | true | config, database, user, session | Show columns headers in output |
| pg_task.string | bool | true | config, database, user, session | Quote only strings |
| pg_conf.close | int | 60 * 1000 | config, database, superuser, session | Close conf, milliseconds |
| pg_conf.fetch | int | 10 | config, database, superuser, session | Fetch conf rows at once |
| pg_conf.restart | int | 60 | config, database, superuser, session | Restart conf interval, seconds |
| pg_task.count | int | 0 | config, database, user, session | Non-negative maximum count of tasks, are executed by current background worker process before exit |
| pg_task.fetch | int | 100 | config, database, user, session | Fetch task rows at once |
| pg_task.id | bigint | 0 | session | Current task id |
| pg_task.limit | int | 1000 | session | Limit task rows at once |
| pg_task.max | int | 0 | config, database, user, session | Maximum count of concurrently executing tasks in group, negative value means pause between tasks in milliseconds |
| pg_task.sleep | int | 1000 | config, database, user, session | Check tasks every sleep milliseconds |
| pg_work.close | int | 60 * 1000 | config, database, superuser, session | Close work, milliseconds |
| pg_work.fetch | int | 100 | config, database, superuser, session | Fetch work rows at once |
| pg_work.restart | int | 60 | config, database, superuser, session | Restart work interval, seconds |
| pg_task.active | interval | 1 hour | config, database, user, session | Positive period after plan time, when task is active for executing |
| pg_task.data | text | postgres | config | Database name for tasks table |
| pg_task.delimiter | char | \t | config, database, user, session | Results columns delimiter |
| pg_task.escape | char | | config, database, user, session | Results columns escape |
| pg_task.group | text | group | config, database, user, session | Task grouping by name |
| pg_task.json | json | [{"data":"postgres"}] | config | Json configuration, available keys: data, reset, schema, table, sleep and user |
| pg_task.live | interval | 0 sec | config, database, user, session | Non-negative maximum time of live of current background worker process before exit |
| pg_task.null | text | \N | config, database, user, session | Null text value representation |
| pg_task.quote | char | | config, database, user, session | Results columns quote |
| pg_task.repeat | interval | 0 sec | config, database, user, session | Non-negative auto repeat tasks interval |
| pg_task.reset | interval | 1 hour | config, database, user, session | Interval of reset tasks |
| pg_task.schema | text | public | config, database, user, session | Schema name for tasks table |
| pg_task.table | text | task | config, database, user, session | Table name for tasks table |
| pg_task.timeout | interval | 0 sec | config, database, user, session | Non-negative allowed time for task run |
| pg_task.user | text | postgres | config | User name for tasks table |
| pg_work.active | interval | 1 week | config, database, user, session | Tasks filter interval for partitioning |

`pg_task` creates table with folowing columns

| Name | Type | Nullable? | Default | Description |
| --- | --- | --- | --- | --- |
| id | bigserial | NOT NULL | autoincrement | Primary key |
| parent | bigint | NULL | pg_task.id | Parent task id (if exists, like foreign key to id, but without constraint, for performance) |
| plan | timestamptz | NOT NULL | CURRENT_TIMESTAMP | Planned date and time of start |
| start | timestamptz | NULL | | Actual date and time of start |
| stop | timestamptz | NULL | | Actual date and time of stop |
| active | interval | NOT NULL | pg_task.active | Positive period after plan time, when task is active for executing |
| live | interval | NOT NULL | pg_task.live | Non-negative maximum time of live of current background worker process before exit |
| repeat | interval | NOT NULL | pg_task.repeat | Non-negative auto repeat tasks interval |
| timeout | interval | NOT NULL | pg_task.timeout | Non-negative allowed time for task run |
| count | int | NOT NULL | pg_task.count | Non-negative maximum count of tasks, are executed by current background worker process before exit |
| hash | int | NOT NULL | generated by group and remote | Hash for identifying tasks group |
| max | int | NOT NULL | pg_task.max | Maximum count of concurrently executing tasks in group, negative value means pause between tasks in milliseconds |
| pid | int | NULL | | Id of process executing task |
| state | enum state (PLAN, TAKE, WORK, DONE, STOP) | NOT NULL | PLAN | Task state |
| delete | bool | NOT NULL | pg_task.delete | Auto delete task when both output and error are nulls |
| drift | bool | NOT NULL | pg_task.drift | Compute next repeat time by stop time instead by plan time |
| header | bool | NOT NULL | pg_task.header | Show columns headers in output |
| string | bool | NOT NULL | pg_task.string | Quote only strings |
| delimiter | char | NOT NULL | pg_task.delimiter | Results columns delimiter |
| escape | char | NOT NULL | pg_task.escape | Results columns escape |
| quote | char | NOT NULL | pg_task.quote | Results columns quote |
| data | text | NULL | | Some user data |
| error | text | NULL | | occured error |
| group | text | NOT NULL | pg_task.group | Task grouping by name |
| input | text | NOT NULL | | sql to execute |
| null | text | NOT NULL | pg_task.null | Null text value representation |
| output | text | NULL | | received result |
| remote | text | NULL | | connect to remote database (if need) |

but you may add any needed colums and/or make partitions

by default `pg_task` runs on default database with default user with default schema with default table with default timeout

to run specific database and/or specific user and/or specific schema and/or specific table and/or specific timeout, set config (in json format)
```conf
pg_task.json = '[{"data":"database1"},{"data":"database2","user":"username2"},{"data":"database3","schema":"schema3"},{"data":"database4","table":"table4"},{"data":"database5","timeout":100}]'
```

if database and/or user and/or schema and/or table does not exist then `pg_task` create it/their
