# pg_scheduler configuration
to run postgres scheduler add it to line

shared_preload_libraries = 'pg_scheduler'

by default postgres scheduler run on all local databases with database owners

to run only specific databases set line

pg_scheduler.database = 'database1,database2:user2'
