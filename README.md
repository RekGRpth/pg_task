# pg_scheduler configuration
to run postgres scheduler add it to line
```conf
shared_preload_libraries = 'pg_scheduler'
```

by default postgres scheduler run on all local databases with database owners

to run only specific databases set line
```conf
pg_scheduler.database = 'database1,database2:user2'
```

by default postgres scheduler use schema in search path and table task

to specify schema and/or table use lines
```conf
pg_scheduler_schema.database1 = schema3
pg_scheduler_table.database1 = table3
```

by default postgres scheduler runs every 1000 ms

to specify other use lines
```conf
pg_scheduler_period.database1 = 100
pg_scheduler_period.database2 = 10
```
