# pg_task configuration
to run pg_task add it to line
```conf
shared_preload_libraries = 'pg_task'
```

bt default pg_task use table `task` and period `1000 ms`

by default pg_task run on local database `postgres` with user `postgres` with default schema (in search path) with default table (as abow) and default period (as abow)

to run specific database and/or user and/or schema and/or table and/or period set line (in json format)
```conf
pg_task.config = '[{"data":"database1"},{"data":"database2","user":"username2"},{"data":"database3","schema":"schema3"},{"data":"database4","table":"table4"},{"data":"database5","period":100}]'
```

if database and/or user and/or schema and/or table does not exist then pg_task create it
