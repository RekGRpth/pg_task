# pg_task configuration
to run postgres task add it to line
```conf
shared_preload_libraries = 'pg_task'
```

by default postgres task run on all local databases with database owners

to run only specific databases set line
```conf
pg_task.database = 'database1,database2:user2'
```

by default postgres task use schema in search path and table task

to specify schema and/or table use lines
```conf
pg_task_schema.database1 = schema3
pg_task_table.database1 = table3
```

by default postgres task runs every 1000 ms

to specify other use lines
```conf
pg_task_period.database1 = 100
pg_task_period.database2 = 10
```
