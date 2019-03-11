# pg_scheduler configuration
to run postgres scheduler add it to line

shared_preload_libraries = 'pg_scheduler'

by default postgres scheduler run on all local databases with database owners

to run only specific databases set line

pg_scheduler.database = 'database1,database2:user2'

by default postgres scheduler use schema in search path and table task

to specify schema and/or table use lines

pg_scheduler_schema.database1 = schema3

pg_scheduler_table.database1 = table3
