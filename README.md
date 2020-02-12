# pg_task config
to run pg_task add it to line
```conf
shared_preload_libraries = 'pg_task'
```

bt default pg_task use table
```conf
pg_task.task = 'task'
```
and period
```conf
pg_task.tick = 1000
```
milliseconds

by default pg_task run on local database `postgres` with user `postgres` with default schema (in search path) with default table (as abow) and default period (as abow)

to run specific database and/or user and/or schema and/or table and/or period set line (in json format)
```conf
pg_task.config = '[{"data":"database1"},{"data":"database2","user":"username2"},{"data":"database3","schema":"schema3"},{"data":"database4","table":"table4"},{"data":"database5","period":100}]'
```

if database and/or user and/or schema and/or table does not exist then pg_task create it

# pg_task using

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

if some queue needs concurently run only 2 tasks then use command
```sql
INSERT INTO task (queue, max, request) VALUES ('queue', 2, 'SELECT now()')
```

if in this queue there are more tasks and they are executing concurently by 2 then command
```sql
INSERT INTO task (queue, max, request) VALUES ('queue', 3, 'SELECT now()')
```
will execute task as more early in this queue

# pg_task install

need this patch
```diff
diff --git a/src/pl/plpgsql/src/pl_handler.c b/src/pl/plpgsql/src/pl_handler.c
index ce03f1ef84..546ae27ad4 100644
--- a/src/pl/plpgsql/src/pl_handler.c
+++ b/src/pl/plpgsql/src/pl_handler.c
@@ -271,6 +271,13 @@ plpgsql_call_handler(PG_FUNCTION_ARGS)
                /* Decrement use-count, restore cur_estate, and propagate error */
                func->use_count--;
                func->cur_estate = save_cur_estate;
+
+               /*
+                * Disconnect from SPI manager
+                */
+               if ((rc = SPI_finish()) != SPI_OK_FINISH)
+                       elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc));
+
                PG_RE_THROW();
        }
        PG_END_TRY();
@@ -368,6 +375,12 @@ plpgsql_inline_handler(PG_FUNCTION_ARGS)
                /* ... so we can free subsidiary storage */
                plpgsql_free_function_memory(func);
 
+               /*
+                * Disconnect from SPI manager
+                */
+               if ((rc = SPI_finish()) != SPI_OK_FINISH)
+                       elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc));
+
                /* And propagate the error */
                PG_RE_THROW();
        }
```
