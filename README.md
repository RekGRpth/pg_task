# pg_task

PostgreSQL, Greenplum and Greengage job scheduler `pg_task` allows to execute any sql command at any specific time at background asynchronously.

It runs entirely inside the database as a set of background workers — no external daemon, no client library, nothing to babysit outside PostgreSQL itself. You enable it with a single GUC, and from then on scheduling a job is just an `INSERT` into a plain table: `pg_task` polls that table, runs `input`, and writes the result back into the same row (see [Task state machine](#task-state-machine) for exactly how).

## Table of contents

- [Quick start](#quick-start)
- [Build](#build)
- [Configuration (GUCs)](#configuration-gucs)
- [Task table](#task-table)
- [Running in multiple databases](#running-in-multiple-databases)
- [Architecture](#architecture)
- [Patterns](#patterns)
- [Capabilities and limitations](#capabilities-and-limitations)
- [Security considerations](#security-considerations)

## Quick start

First, enable the extension by adding it to `shared_preload_libraries` and restarting PostgreSQL:
```conf
shared_preload_libraries = 'pg_task' # add pg_task to shared_preload_libraries
```
On startup, `pg_task` sets up everything it needs by itself — role, database, schema and the `task` table — using built-in defaults (database `postgres`, user `postgres`, schema `public`, table `task`; see [Configuration (GUCs)](#configuration-gucs) to point it elsewhere, and [Self-provisioning and the helper triggers](#self-provisioning-and-the-helper-triggers) for how).

Second, schedule work by inserting rows into the `task` table — one row is one job:
```sql
INSERT INTO task (input) VALUES ('SELECT now()'); -- no plan/repeat: runs once, as soon as possible
INSERT INTO task (plan, input) VALUES (now() + '5 min':INTERVAL, 'SELECT now()'); -- runs once, after a 5 minute delay (plan = planned time)
INSERT INTO task (plan, input) VALUES ('2029-07-01 12:51:00', 'SELECT now()'); -- runs once, at that exact timestamp (plan = planned time)
INSERT INTO task (repeat, input) VALUES ('5 min', 'SELECT now()'); -- runs, then reinserts itself to run again every 5 minutes (repeat = interval)
INSERT INTO task (input) VALUES ('SELECT 1/0'); -- an error doesn't crash the worker: it's caught and written to error as text
INSERT INTO task (group, max, input) VALUES ('group', 1, 'SELECT now()'); -- max = 1 lets one extra task of this group run concurrently with this one (2 at a time total)
INSERT INTO task (group, max, input) VALUES ('group', 2, 'SELECT now()'); -- a higher max also jumps the queue ahead of lower-max tasks in the same group — it behaves like a priority, not just a concurrency cap
INSERT INTO task (input, remote) VALUES ('SELECT now()', 'user=user host=host'); -- remote runs input on another database instead of the local one
UPDATE task SET state = 'STOP' WHERE id = ...; -- cancels a running (state = WORK) task, local or remote; input's query gets cancelled and state stays STOP, not FAIL
```
`pg_task` notices a new or changed row almost immediately — no polling delay to wait out (see [Wake-up and crash recovery](#wake-up-and-crash-recovery)) — executes `input`, and writes the outcome straight back into that row: the result goes to `output`, any error to `error`, and `state` tracks progress along the way (`PLAN → TAKE → WORK → DONE`/`FAIL`). There's no separate status API — just query the table: `SELECT * FROM task WHERE id = ...`.

## Build

`pg_task` is a standard [PGXS](https://www.postgresql.org/docs/current/extend-pgxs.html) extension, built against an installed PostgreSQL, Greenplum or Greengage server.

Requirements: matching `-dev`/`-devel` package with `pg_config` on `PATH`, a C compiler, `make`, `curl` and `pcregrep`.

```sh
make USE_PGXS=1 install
```

Before compiling, the build auto-generates `postgres.c` (and `exec.c` from it) by detecting the installed server's flavor and version (`postgres --version`, `pg_config --version`/`--gp_version`) and downloading the matching `src/backend/tcop/postgres.c` from the corresponding upstream repository (`postgres/postgres` or `GreengageDB/greengage`) on GitHub.

If you already have the exact source tree the server was built from (e.g. a custom/unreleased build), you can skip the network fetch: place a symlink named `postgres.c` pointing at `src/backend/tcop/postgres.c` in that tree before running `make` — an existing `postgres.c` is used as-is.

## Configuration (GUCs)

`pg_task` creates the following GUCs. `Level` lists where each one can be set; when a GUC is settable at more than one level, the most specific value wins — a per-session `SET` beats a per-role/database default, which beats the config file. Several `task` columns (see [Task table](#task-table) below) default to the matching GUC's current value at insert time, so setting the GUC once is often enough without repeating it on every row.

| Name | Type | Default | Level | Description |
| --- | --- | --- | --- | --- |
| pg_task.delete | bool | true | config, database, user, session | Auto delete task when both output and error are nulls |
| pg_task.drift | bool | false | config, database, user, session | Compute next repeat time by stop time instead by plan time |
| pg_task.header | bool | true | config, database, user, session | Show columns headers in output (only when the query returns at least one row and more than one column) |
| pg_task.save | bool | false | config, database, user, session | Save session state between tasks |
| pg_task.spi | bool | false | config, database, user, session | SPI (or local) execution? Also affects `input` containing multiple `;`-separated statements: SPI mode runs each statement separately and appends all results, same as local mode |
| pg_task.string | bool | true | config, database, user, session | Quote only strings |
| pg_conf.fetch | int | 10 | config, database, superuser | Fetch conf rows at once |
| pg_conf.max | int | max_worker_processes | config | Maximum task and work workers |
| pg_conf.restart | int | 60 | config | Restart pg_conf after it crashed in that many seconds (read once, at server start) |
| pg_task.count | int | 0 | config, database, user, session | Non-negative maximum count of tasks, are executed by current background worker process before exit |
| pg_task.fetch | int | 100 | config, database, user | Fetch task rows at once |
| pg_task.id | bigint | 0 | session | Current task id (for read only) |
| pg_task.limit | int | 1000 | config, database, user | Limit task rows at once |
| pg_task.max | int | 0 | config, database, user, session | Maximum count of additional concurrently executing tasks in group (total concurrency = max + 1), negative value means pause between tasks in milliseconds |
| pg_task.run | int | 2147483647 | config, database, user, session | Maximum count of concurrently executing tasks in work |
| pg_task.sleep | int | 1000 | config, database, user | Check tasks every sleep milliseconds |
| pg_work.fetch | int | 100 | config, database, superuser | Fetch work rows at once |
| pg_work.idle | int | 60 | config, database, user | Empty passes after which pg_work goes idle: waits for the next task planned or a wake-up rather than polling every `sleep`, though no longer than `idle` × `sleep`, for a task it may have missed |
| pg_work.restart | int | 60 | config, database, user | Restart pg_work after it crashed in that many seconds (that of the role and database of its entry, read when pg_conf starts it) |
| pg_task.active | interval | 1 hour | config, database, user, session | Positive period after plan time, when task is active for executing |
| pg_task.data | text | postgres | config | Database name for tasks table |
| pg_task.delimiter | char | \t | config, database, user, session | Results columns delimiter, nothing between them if empty |
| pg_task.escape | char | | config, database, user, session | Results columns escape |
| pg_task.group | text | group | config, database, user, session | Task grouping by name |
| pg_task.json | json | [{"data":"postgres"}] | config | Json configuration, available keys: data, reset, run, schema, sleep, spi, table and user |
| pg_task.live | interval | 0 sec | config, database, user, session | Non-negative maximum time of live of current background worker process before exit |
| pg_task.null | text | \N | config, database, user, session | Null text value representation |
| pg_task.plan | timestamptz | statement_timestamp() | config, database, user, session (superuser only) | Default value for plan timestamp, and what the scheduler takes for now: an SQL expression, run as `pg_task.user`, so only a superuser may set it |
| pg_task.quote | char | | config, database, user, session | Results columns quote |
| pg_task.repeat | interval | 0 sec | config, database, user, session | Non-negative auto repeat tasks interval |
| pg_task.reset | interval | 1 hour | config, database, user | Interval of reset tasks; a value that isn't a positive, finite interval is refused as set, as it would keep `pg_conf` from applying `pg_task.json` for every database (one set before this check, still in `pg_db_role_setting`, has to be fixed by hand: `pg_conf` logs `pg_task.json not applied` with it) |
| pg_task.schema | text | public | config, database, user | Schema name for tasks table |
| pg_task.table | text | task | config, database, user | Table name for tasks table |
| pg_task.timeout | interval | 0 sec | config, database, user, session | Non-negative allowed time for task run |
| pg_task.user | text | postgres | config | User name for tasks table |

## Task table

`pg_task` creates the `task` table (name and location configurable, see above) with the following columns. Most of them default to the corresponding GUC and can be overridden per row — so a task can, for example, use a longer `timeout` or a different `group` than the session default just by setting that column on insert.

| Name | Type | Nullable? | Default | Description |
| --- | --- | --- | --- | --- |
| id | bigserial | NOT NULL | autoincrement | Primary key |
| parent | bigint | NULL | pg_task.id | Parent task id (if exists, like foreign key to id, but without constraint, for performance) |
| plan | timestamptz | NOT NULL | pg_task.plan | Planned date and time of start; `infinity` holds a task back till its `plan` is set to a time (see the pattern of a parent task below), `-infinity` makes it due at once, with no pause of its group (`max < 0`) after it, and the next of a repeating one planned from now |
| start | timestamptz | NULL | | Actual date and time of start |
| stop | timestamptz | NULL | | Actual date and time of stop |
| active | interval | NOT NULL | pg_task.active | Positive period after plan time, when task is active for executing |
| live | interval | NOT NULL | pg_task.live | Non-negative maximum time of live of current background worker process before exit |
| repeat | interval | NOT NULL | pg_task.repeat | Non-negative auto repeat tasks interval |
| timeout | interval | NOT NULL | pg_task.timeout | Non-negative allowed time for task run; one that passes the check, which counts a month as 30 days, but is less than nothing in milliseconds, which count a year as 365.25 days (`'-60 mon 1800 days'`, say), is taken as none, as 0 is |
| count | int | NOT NULL | pg_task.count | Non-negative maximum count of tasks, are executed by current background worker process before exit |
| max | int | NOT NULL | pg_task.max | Maximum count of additional concurrently executing tasks in group (total concurrency = max + 1), negative value means pause between tasks in milliseconds |
| pid | int | NULL | | Id of process executing task |
| state | enum state (PLAN, GONE, TAKE, WORK, DONE, FAIL, STOP) | NOT NULL | PLAN | Task state |
| delete | bool | NOT NULL | pg_task.delete | Auto delete task when both output and error are nulls |
| drift | bool | NOT NULL | pg_task.drift | Compute next repeat time by stop time instead by plan time |
| header | bool | NOT NULL | pg_task.header | Show columns headers in output (only when the query returns at least one row and more than one column) |
| save | bool | NOT NULL | pg_task.save | Save session state between tasks; with false, it's reset before the next task of the worker as `DISCARD ALL` does, the session's advisory locks let go of too; with true, the `search_path` a task sets carries over too, and in local and spi mode its `statement_timeout` (for the next tasks with no `timeout` of their own), while that of a remote task is set anew for each from its `timeout` |
| string | bool | NOT NULL | pg_task.string | Quote only strings |
| delimiter | char | NOT NULL | pg_task.delimiter | Results columns delimiter, nothing between them if empty |
| escape | char | NOT NULL | pg_task.escape | Results columns escape |
| quote | char | NOT NULL | pg_task.quote | Results columns quote |
| data | text | NULL | | Some user data |
| error | text | NULL | | Catched error, with the detail the client gets, not the one for the server log only (which, for a deadlock, has the queries of the other sessions in it); with `output`, up to 16 MB short of 1 GB together, the error kept first and the output cut to what room is left |
| group | text | NOT NULL | pg_task.group | Task grouping by name |
| input | text | NOT NULL | | Sql command(s) to execute |
| null | text | NOT NULL | pg_task.null | Null text value representation |
| output | text | NULL | | Received result(s), up to 16 MB short of 1 GB, with `error` if any: a task that outputs more fails with `task output exceeds ... bytes`, keeping that much |
| remote | text | NULL | | Connect to remote database (if need) |
| user | name | NOT NULL | current_user | Role that inserted the task; input is executed as this role, and the column is immutable after insert |

You may freely add your own columns to `task` and/or partition it — `pg_task` only ever touches the columns it created (see [Self-provisioning and the helper triggers](#self-provisioning-and-the-helper-triggers)). The repeat of a task copies your columns too, as the table has them at that moment, except generated and identity ones, which it leaves to the table to fill; your own `CHECK` constraints, on `pg_task`'s columns too, are left alone; and a column named `hash` is dropped only if it's the legacy one earlier versions of `pg_task` made, not one of yours.

## Running in multiple databases

By default `pg_task` runs a single scheduler, on the default database (`postgres`), as the default user (`postgres`), watching the default schema (`public`) and table (`task`), polling every default `sleep` interval.

To run more than one scheduler — e.g. one per application database, each with its own user/schema/table/poll interval — list them in `pg_task.json`, one object per scheduler; any key you omit falls back to its GUC default:
```conf
pg_task.json = '[{"data":"database1"},{"data":"database2","user":"username2"},{"data":"database3","schema":"schema3"},{"data":"database4","table":"table4"},{"data":"database5","sleep":100}]'
```
`pg_task` creates whichever of the referenced database, user, schema or table don't already exist — you don't need to provision them by hand first.

Each of an entry's scheduler settings — `sleep`, `reset`, `run`, `spi`, `limit`, `schema` and `table`, and `pg_work.restart` (which has no key in `pg_task.json`) — comes from the first of: its key in the entry's `pg_task.json` object, `ALTER ROLE <user> IN DATABASE <data> SET pg_task.…`, `ALTER ROLE <user> SET pg_task.…`, `ALTER DATABASE <data> SET pg_task.…`, and the server's configuration (`postgresql.conf`, `ALTER SYSTEM`) — the order PostgreSQL itself applies them in to a session of that role in that database. Settings of other roles in that database don't apply to it, and neither, on PostgreSQL 9.5+, do those of the database and role `pg_conf` itself runs in (`postgres` and the bootstrap superuser). `pg_conf` and the entry's `pg_work` read them again on every reload (`SIGHUP`), so an `ALTER ROLE`/`ALTER DATABASE ... SET` or `RESET` of one takes effect without a restart — except, before 9.5, a `RESET` on the role or database `pg_work` itself runs in, whose session keeps the value it got on connecting until it restarts.

A `pg_task.json` that doesn't parse, or whose values don't fit the types of their keys, is logged and left unapplied: `pg_conf` and the running `pg_work` workers carry on with the configuration they have until it's fixed.

## Architecture

`pg_task` has no `pg_task--<version>.sql` control script and is never activated with `CREATE EXTENSION` — everything it needs (role, database, schema, table, the `state` enum, indexes, defaults, constraints, its triggers and, on PostgreSQL 9.5+, a row level security policy) is created idempotently by the extension itself the first time it starts, by checking `pg_catalog` before every `CREATE`/`ALTER` and only touching what's missing or mismatched. Rerunning it, or adding your own columns/partitions by hand, is safe — `pg_task` never drops or rewrites what it didn't create itself.

### Process hierarchy

The extension is split into four parts, each backed by its own source file and, except the first, its own background worker type:

1. **`init`** registers the extension's GUCs and, once, the single static background worker `pg_conf` (started under `shared_preload_libraries`).
2. **`pg_conf`** (one process per postmaster; on Green(plum|gage), coordinator only) parses `pg_task.json` — together with any per-database/per-role overrides, see [Running in multiple databases](#running-in-multiple-databases) — on start and on every reload (`SIGHUP`), creates the referenced role/database if missing, launches one dynamic background worker `pg_work` per `{data, schema, table, user, sleep, ...}` entry, and stops those whose entry is gone. If it crashes, the postmaster restarts it after `pg_conf.restart` seconds.
3. **`pg_work`** (one process per such entry) is the scheduler proper: it provisions the schema/table on first connect (see below), then loops on a wait-event set — its latch plus the sockets of any open remote connections — periodically claiming due rows from the task table (respecting `plan`, `active` and per-group concurrency), expiring overdue non-repeating rows to `GONE`, and resetting rows orphaned by a crashed worker back to `PLAN`. Local tasks (no `remote`) are handed off to a child `pg_task` worker; remote tasks are driven directly by `pg_work` over an async, non-blocking `libpq` connection — no extra OS process per remote task. Neither `pg_work` nor `pg_conf`, which run no code of task authors, keep the `transaction_timeout` (PostgreSQL 17+) of the database or role settings, which is for the tasks' `input`.
4. **`pg_task`** (one process per concurrently running local task) connects, executes `input` (locally or via SPI, see below), writes `output`/`error`, flips `state` to `DONE`/`FAIL`, schedules the next `repeat` occurrence if any, and exits — or, within `count`/`live`, picks up another task of the same group and the same author before exiting, one whose own `max` the group, with the tasks of it running now (rows in `TAKE`/`WORK`), fits, as for any task taken: tasks of a higher `max` may have filled it since.

Parameters are passed down the hierarchy through a fixed pool of slots in shared memory, `pg_conf.max` of them, set up at server start: the starting process fills a free slot and hands its number to the new worker as `bgw_main_arg`, and the slot is freed once the worker is done with it — not through command-line arguments or files.

### Task state machine

`PLAN → TAKE → WORK → DONE | FAIL`, or `PLAN → GONE` when a non-repeating task's `active` window elapses before it's picked up (typically: the server was down longer than `active` allows). `STOP` is a manual, terminal state you set yourself (`UPDATE task SET state = 'STOP' WHERE id = ...`); no code transitions a row into it automatically. Setting it on a `PLAN` row just keeps it from ever being claimed (it's filtered out by `state = 'PLAN'` like any other non-`PLAN` row). Setting it on a `WORK` row actually cancels the running task: a trigger wakes the owning `pg_work` (the same advisory-lock mechanism as the wake-up trigger below), which within `pg_task.sleep` cancels the task's query once — for a local task by marking the task in its worker's shared memory slot and sending the worker `SIGUSR2`, which it turns into a query cancel only while running the `input` of that very task, sending `SIGINT` to its process group too, as `pg_cancel_backend()` does, so that the programs the `input` started (`COPY ... PROGRAM`) are interrupted rather than waited for — not a next task it may have taken on by then with `count`/`live`, nor the bookkeeping of this one (what a procedure or a `DO` block runs after a `COMMIT` of its own, and the deferred triggers of the input's commit, in `pg_task.spi` mode as in local mode, are the input still, which `STOP` and a termination cut short too, and its `timeout` all but the deferred triggers, as `statement_timeout` doesn't reach the commit in PostgreSQL itself) (and with no role checks to pass, unlike `pg_cancel_backend()`, as the worker runs as the task's author, whom `pg_task.user` may not be allowed to signal), for a `remote` task by sending a cancel request on that connection — with libpq 17+ asynchronously, from `pg_work`'s wait loop, so that a server slow to answer it doesn't hold `pg_work` up (and the connection, a cancel on its way to it, isn't reused for the next task of the group, which the cancel would reach instead), and with the blocking `PQcancel()` before that. Either way the cancelled query's error is caught the normal way, but the row is left at `STOP` instead of being overwritten to `FAIL`, and — unlike a plain `FAIL` — its next `repeat` occurrence, if any, is not inserted. There's still no way to cancel a task from SQL other than this — `pg_cancel_backend(pid)` directly works too, but leaves the row at `FAIL` since nothing marked it `STOP` first.

This diagram is enforced, not just documented: a `BEFORE UPDATE OF "state"` trigger (`task_state`) allows exactly `PLAN → {TAKE, GONE, STOP}`, `TAKE → {WORK, PLAN, DONE, FAIL}`, `WORK → {DONE, FAIL, PLAN, STOP}` — and only to the table owner (`pg_task.user`, whose bookkeeping drives a task through them), its members and superusers; any other role, a task author say, may only set a `PLAN` or `WORK` task to `STOP` — and nothing else — including no transition at all out of `DONE`, `FAIL`, `GONE` or `STOP`, or out of any state you might add to the enum yourself (see Patterns). Worth knowing before reaching for a custom state of your own: once a row is in one, there's no trigger-sanctioned way to move it back out.

`PLAN → TAKE` is a single `UPDATE ... SKIP LOCKED` that also counts current concurrency for the task's `group`/`remote` hash — via session-level advisory locks visible in `pg_locks`, and for local tasks via the shared memory slots of their workers too, each worker counted once, which keep counting even if an input let go of the locks with `pg_advisory_unlock_all()` and the `pg_work` that started it, holding them as well, was restarted since — against `max`. So `max` behaves less like a hard cap feeding one shared queue and more like a priority: a group with a higher `max` picks up its own next tasks sooner, independently of other groups. A negative `max` instead schedules a pause: on completion, the other `PLAN` rows of the same group planned within the pause — already due or not — get their `plan` pushed to its end: `|max|` milliseconds after now with `drift`, or else, as `repeat` does, the first time after now that is a multiple of `|max|` milliseconds after the `plan` of the task just done, so that the group keeps its pace. The pause holds the tasks of the group with a negative `max` inserted or planned later on too, till its end, which `pg_work` doesn't take any sooner — a producer inserting its tasks one at a time gets them paced as well; it's kept in shared memory, one per group at a time, for as many groups at once as `pg_conf.max` (beyond that, the pauses that end soonest are dropped), and not kept across a restart of the server. `pg_task.run` (`config`/`database`/`user` only — unlike `max`, there's no per-row override) caps how many tasks this one `pg_work` may have in flight at once across *all* groups combined, on top of whatever `max` allows any individual group — the one concurrency limiter in `pg_task` that isn't per-group. Local tasks are also taken no more than there are free slots in the shared pool (`pg_conf.max`), one per task worker; remote tasks need none. A local task that still finds no free slot or background worker — `max_worker_processes` is shared with other workers and other `pg_work`s — goes back to `PLAN` with a `WARNING` naming the setting to raise rather than to `FAIL`, and is run once a worker frees up. A remote task, from PostgreSQL 13 on, needs a file descriptor for its connection instead, out of those `pg_work` may hold for others than files, about a third of `max_files_per_process`: one that finds none left, every one taken by the other remote tasks of this `pg_work`, likewise goes back to `PLAN` with a `WARNING`, and is run once one of them is done.

`WORK → DONE/FAIL` is a single `UPDATE ... RETURNING` that, in the same round trip, decides whether to delete the row (`delete`, when both `output` and `error` are null), whether to insert the next `repeat` occurrence (computed from the original `plan` or from the actual finish time, depending on `drift`), whether the same worker process may pick up another task of the group without exiting (within `count`/`live`), and whether to reschedule the rest of the group (negative `max`). Without `drift`, the next occurrence is the first `plan + repeat + repeat + ...` past now, a repeat added at a time, as a month after a month isn't two months; one more than 1000 repeats behind jumps the rest of the way in one go, by as many repeats as the time left takes at the pace the plan kept so far, the first past now of that one and the ones a repeat before and after it, which may land a repeat off across changes of daylight saving time or months of other lengths. A `repeat` with parts of both signs (`'-1 mon 31 days'`), which may take the plan nowhere or back, doesn't jump: one that far behind, or one that still lands at or before now, is planned its length (`EXTRACT(epoch FROM repeat)`, a second at least) after now.

`timeout` bounds how long `input` itself may run — locally via a timeout event in the `pg_task` worker's loop, remotely via `SET SESSION statement_timeout` sent ahead of `input`; a local task's `timeout` is capped by the server's `statement_timeout`, and `0` leaves that one in effect (remotely, the remote server's). `live`/`count` instead bound the executor *process*, not the task: how many tasks in a row, or how long, one `pg_task` worker lives before being recycled.

### Wake-up and crash recovery

Instead of `LISTEN`/`NOTIFY`, `pg_task` wakes idle workers with session-level advisory locks plus `pg_cancel_backend()`. Each `pg_work` process holds an advisory lock tagged with its group's hash for as long as it's alive; the `AFTER INSERT OR DELETE OR UPDATE OF plan` trigger (see below) looks up the holder of that lock in `pg_locks` and cancels it directly. `pg_work` installs its own `SIGINT` handler — instead of the default query-cancel one — that just sets the latch, so the wait-event loop returns immediately instead of waiting out the rest of `pg_task.sleep`. The `STOP` trigger (see [Task state machine](#task-state-machine)) reuses this exact same wake-up: once woken, `pg_work` checks, at most once per `pg_task.sleep`, whether any still running task now has `state = 'STOP'`, and cancels it once — a remote one with a cancel request on its connection, a local one through its worker's slot and `SIGUSR2`, as described there.

A second, per-task advisory lock (tagged by the task's own `id`) is used to detect a crashed executor: every `pg_task.reset` interval, `pg_work` looks for rows still in `TAKE`/`WORK` whose `id`-tagged lock nobody currently holds, and resets them to `PLAN` — unless a live task worker still has the task in its shared memory slot, which an input can't let go of, as `pg_advisory_unlock_all()` or `DISCARD ALL` in it let go of the locks of `pg_task` too (user locks, not advisory ones, but in the same lock method); those the worker takes back after each command of the input. That's the crash-recovery mechanism. A crashed `pg_work` itself is restarted by the postmaster after `pg_work.restart` seconds; it first checks `pg_task.json` again, and exits for good if its entry has been removed meanwhile.

When there's genuinely nothing to do, `pg_work` doesn't poll in a tight loop: it computes, in one query, the soonest moment something will actually need attention — the closer of the next `active`/`timeout` deadline among running tasks and the next `PLAN` task's `plan` — and sleeps exactly until then (at most about 24.8 days, `INT_MAX` milliseconds, at a time, after which it works the moment out again; before PostgreSQL 9.6, where a wake-up of its own copy of the 9.6 latch code may be lost, a `pg_task.sleep` at most). `pg_task.sleep` is a floor on responsiveness for a busy queue, not a fixed polling interval.

### Self-provisioning and the helper triggers

On first connect, `pg_work` walks through a series of idempotent `SELECT EXISTS ...` checks against `pg_catalog` and issues the matching `CREATE`/`ALTER` only for what's missing: schema, the `state` enum, the table (with all columns, `current_setting('pg_task.…')`-backed defaults, `NOT NULL`/`CHECK` constraints), and indexes — including a functional index on the hash of `group`/`remote` that the concurrency accounting above relies on — plus 27 trigger functions and their triggers, named `<table>_<suffix>` (e.g. `task_state`); for a table name too long for that to fit in `NAMEDATALEN`, the table part is shortened and followed by a hash of the table, e.g. `…_365fef26_state`, so that the names stay distinct. A trigger function or trigger that exists but differs from what this version of `pg_task` makes — a function's body, `SECURITY DEFINER` or `search_path`, a trigger's timing, events, `FOR EACH ROW`/`STATEMENT` or `UPDATE OF` column — is re-created too, so that a table made by an earlier version is brought up to date. Each such statement waits for a table busy with long transactions of others (writing to it, say) 2 seconds at most, a `lock_timeout` for its own transaction only, retried five times, after which `pg_work` exits, to be restarted after `pg_work.restart`; apart from that, `pg_work`'s own queries don't go by the server's `lock_timeout` and `statement_timeout`, as the scheduler stays running however long others hold its table. Constraints, defaults and indexes are recognized by comparing their text, as `pg_get_expr()` deparses it, with `pg_task`'s own, so `pg_work` first sets `IntervalStyle` and `quote_all_identifiers` to their defaults in its own session, whatever the server, its database or its role sets. Four of them do something beyond plain immutability and are worth calling out individually:

- the **`user`-immutability trigger** (`BEFORE INSERT OR UPDATE OF "user"`) forces `NEW."user"` to `current_user` on insert unless the inserting role is a member of the claimed role or of the table's owner (`pg_task.user`, which inserts the repeats of tasks), and rejects any later change — this is what makes the `user` column trustworthy for the Security considerations below.
- the **wake-up trigger** (`AFTER INSERT OR DELETE OR UPDATE OF plan`) is the mechanism described above; it does not use `NOTIFY`.
- the **`STOP` trigger** (`AFTER UPDATE OF "state"`) is what makes setting `state = 'STOP'` on a `WORK` row actually cancel it, as described in [Task state machine](#task-state-machine) above.
- the **validity trigger** (`BEFORE INSERT OR UPDATE`, `<table>_valid`) adds `active`, `live`, `repeat` and `timeout` up and to `plan` and to the current time, the way `pg_work` does later on, so that values out of range (an interval of hundreds of thousands of years, say) fail the insert or update with `timestamp out of range`, rather than `pg_work` itself. A `timeout` longer than about 24.8 days (`INT_MAX` milliseconds) works as that long. It also refuses an `active`, `live`, `repeat` or `timeout` with a part less than 0, of parts of both signs (`'-1 mon 31 days'`, more than nothing as it is), which may take the plan nowhere or back on some days, as inserted or changed — not as kept in a row from before this check, which other updates, of its `state` by `pg_work` say, leave be, nor in the copy of a `repeat` that `pg_work` inserts with the intervals of the task before it.

The other 22 are plain `BEFORE UPDATE OF "<column>"` guards, one per remaining column, and fall into three groups:

- `state` is governed by the transition-validation trigger described in [Task state machine](#task-state-machine) above, rather than by immutability.
- `id`, `group`, `remote` and `parent` are immutable unconditionally, from the moment of insert — a task's routing and ancestry can't be edited after the fact, only fixed by inserting a new row instead (see Patterns).
- every other self-provisioned column — `plan`, `active`, `live`, `repeat`, `timeout`, `count`, `max`, `delete`, `drift`, `header`, `save`, `string`, `delimiter`, `escape`, `quote`, `input`, `null`, `data` — is immutable only once the task has left `PLAN`: freely editable on a still-queued task (e.g. `UPDATE task SET input = ... WHERE state = 'PLAN'`), frozen the instant it's dispatched.

None of this applies to columns you add yourself — `pg_task` only ever generates a trigger for a column it created (see Patterns).

On PostgreSQL 9.5+ it also creates a row level security policy on the table, named `<table>_user` like the `user` trigger, and enables row level security — see Security considerations below.

### Three ways to run `input`

There are three distinct execution paths, not two — which one applies is decided first by whether `remote` is set, and only then, for the non-remote case, by `pg_task.spi`:

- **local (no `remote`, default `spi = off`)** dispatches `input` through `exec_simple_query()` in the `pg_task` worker's own backend — the same function extracted from the matching version's `postgres.c` into `exec.c` at build time (see Build above) — i.e. the same multi-statement dispatcher PostgreSQL uses for a real client connection: full DDL, multiple `;`-separated statements, implicit transaction handling. Its result stream is captured by swapping in a custom `DestReceiver` that formats each row (honoring the task's `delimiter`/`quote`/`escape`/`null`/`string`) straight into `output`, turning command-completion tags (`UPDATE 3`, ...) into `output` lines too.
- **SPI (no `remote`, `spi = on`)** calls `SPI_execute()` directly in the same backend, instead of `exec_simple_query()` — faster, but strictly narrower (see the table below). Its `output` reads the same as in the other two modes: a statement's command tag is taken from the statement itself (`CREATE TABLE`, `MERGE 2`, `SELECT 2` for `CREATE TABLE AS`, ...), not from SPI's own result codes.
- **remote (`remote` is set)** bypasses both of the above entirely: `pg_task.spi` is not even read in this path. `pg_work` doesn't spawn a `pg_task` worker at all — it opens the connection itself, asynchronously and non-blocking (`PQconnectStartParams` + `PQsetnonblocking`), and adds its socket to its own wait-event set (a host name in `remote` is looked up by libpq synchronously, though, with `pg_work` and every remote task it runs held up till the DNS answers, for as long as the resolver's timeout if it doesn't: use `hostaddr`, or a local caching resolver, where that matters) — giving up after the `connect_timeout` of `remote`, if any, which libpq leaves to the application for such a connection, with the task failed with `timeout expired` rather than kept in `TAKE` for as long as the TCP timeout; with several hosts in `remote` (libpq 10+), `connect_timeout` applies to each of them, as for a synchronous connection, `pg_work` trying them one at a time, in the order of `load_balance_hosts`, and failing the task only once the last one fails too, with the errors of them all (the addresses a host name resolves to share the `connect_timeout` of their host, rather than having one each) — so dozens of concurrent remote tasks cost no extra OS process beyond `pg_work` itself. Once connected it sends a preamble (`SET SESSION` for the relevant `pg_task.*` parameters and `statement_timeout`) followed by `input`, and `input` is dispatched by the remote server's own query processor over the wire, exactly as if a regular client had sent it — `pg_task`'s local `DestReceiver`/SPI code never runs. An `input` longer than the socket takes at once is sent in parts as the socket becomes writable, and a result is read only once it has arrived in full, so one remote task never holds up `pg_work` or the other tasks. The streamed result is formatted into `output`/`error` the same way as in local mode, statement by statement. It comes in the `client_encoding` of the connection, which is always the encoding of the task table's database — a `client_encoding` in `remote`, as a keyword or in its `options`, is ignored; an `input` that sets another one itself (`SET client_encoding = ...`) has its output and error, the server's messages included, checked to be valid text in that encoding before they are stored, whether the task is done or failed on the way, its connection broken say; whichever isn't is dropped, and the task fails with `invalid byte sequence for encoding ...` after its own error, if any, rather than having its bytes stored as they came (in a database with a single-byte encoding any bytes are valid, so there such an `input` goes unnoticed). Once `input` finishes, `pg_work` sends `COMMIT` (closing whatever transaction `input` left open) and then `DISCARD ALL` unless `save` asks to keep the session for the next task of the group — before the connection is either reused or closed. The notices of the remote server (`RAISE NOTICE`, `WARNING`s) are logged by `pg_work` as its own, prefixed with the task's `id`, at their level (no higher than `WARNING`) and subject to `log_min_messages`, as a local task's are, rather than printed by libpq to `stderr` as they come; the notifications of a `LISTEN` in `input` have nowhere to go and are dropped as they arrive (logged at `DEBUG1`).

In every case the final state transition (`WORK → DONE/FAIL`) is written locally via SPI, regardless of which of the three paths actually ran `input`.

Each path has its own restrictions on what `input` can contain:

| | local | SPI | remote |
| --- | --- | --- | --- |
| Several `;`-separated statements | all results appended to `output` | all results appended to `output` (split via `RawStmt.stmt_location`, or before PostgreSQL 10 by the server's own scanner, at each `;` outside parentheses) | all results appended to `output` |
| `COPY ... FROM STDIN` / `... TO STDOUT` / `COPY BOTH` | rejected outright (`COPY … is not supported`) | rejected outright (`SPI_ERROR_COPY`) | `FROM STDIN`/`BOTH` rejected (pg_task has no data to stream in); `TO STDOUT` **is** supported and streamed straight into `output`, its rows on lines of their own |
| `COPY ... TO/FROM` a server-side file or `PROGRAM` | allowed, if the role has the privilege | allowed, if the role has the privilege (SPI rejects only `STDIN`/`STDOUT`) | allowed, if the role has the privilege — runs on the remote server |
| Explicit `BEGIN`/`COMMIT`/`ROLLBACK` in `input` | allowed (a transaction left open at the end is closed automatically) | rejected (`SPI_ERROR_TRANSACTION`) | allowed (it's a real client session on the far side) |
| DDL | allowed | allowed (as an SPI utility statement) | allowed |
| Statements that can't run inside a transaction block (`VACUUM`, `DISCARD ALL`, `CREATE DATABASE`, ...) | allowed | rejected (SPI runs `input` inside a transaction) | allowed |
| `LOCK TABLE` without `BEGIN` | rejected (only in transaction blocks) | allowed (SPI runs `input` inside a transaction) | rejected (only in transaction blocks) |
| `output` | the rows if a statement returns any, else its command tag with the row count where PostgreSQL reports one (`INSERT 0 1`, `UPDATE 3`, `CREATE TABLE`), nothing for `RETURNING` without rows | the same | the same |

### Signals

`SIGHUP` — in `pg_conf`, `pg_work` and `pg_task` alike — reloads `postgresql.conf` and re-runs the relevant config/task check, so most GUCs and `pg_task.json` can be changed without a server restart. `SIGTERM` terminates each of them at its next safe point (`CHECK_FOR_INTERRUPTS()`), as it does a regular backend, rather than right in the signal handler, as the default one of background workers does, which could come in the middle of a commit; on the way out, each level releases its advisory locks, and `pg_work` additionally cancels its remote tasks still running — with libpq 17+ all at once, waiting a second at most for them together — and closes its remote connections cleanly instead of dropping them. `SIGINT` cancels nothing in `pg_work`: it's the wake-up described in [Wake-up and crash recovery](#wake-up-and-crash-recovery). In a `pg_task` worker `SIGINT` is the usual query cancel (`pg_cancel_backend()`), though only of a task's `input`: one coming in between tasks or during their bookkeeping is for no query, as one coming to an idle backend, and is ignored rather than failing the bookkeeping and the task's result with it; `SIGUSR2` is the cancel of a task set to `STOP` (see [Task state machine](#task-state-machine)).

## Patterns

`pg_task` is intentionally minimal — no built-in retry, backoff or conditional repeat. Both of the recipes below build the missing behavior entirely in SQL, on top of mechanics already described above: the atomicity of running `input` (see Three ways to run `input`), and the fact that `pg_task` only ever touches the columns it created (see Self-provisioning and the helper triggers), so your own columns and triggers on `task` compose freely with it. No core code changes needed for either.

### Retry until success

A repeating task (`repeat > 0`) that should actually run once, and only keep repeating while it keeps failing: make the *last* statement of `input` cancel the row's own `repeat`, referencing the running task's own id via `pg_task.id` (see Configuration (GUCs)):

```sql
INSERT INTO task (input, repeat) VALUES ($$
    -- do the real work; an unhandled error here aborts everything below too
    INSERT INTO some_table (...) VALUES (...);

    -- reached, and committed, only if everything above succeeded
    UPDATE task SET repeat = '0 sec' WHERE id = current_setting('pg_task.id')::bigint;
$$, '1 min');
```

This works because `input` runs as one atomic unit — the local dispatcher treats a `;`-separated `input` sent in one go as a single implicit transaction (see Three ways to run `input`), and SPI mode wraps it in one subtransaction — while the state/`repeat`-scheduling update (`task_done()`/`task_insert()` in `task.c`) always runs afterwards, in its own transaction, and reads whatever `repeat` value actually ended up committed on the row:

- **on success**, the `UPDATE ... repeat = '0 sec'` commits together with the real work, so by the time `pg_task` decides whether to schedule the next occurrence, `repeat` is already `0 sec` — no further occurrence is inserted, and the task effectively ran once.
- **on failure**, the whole `input` — including that trailing `UPDATE` — rolls back together, so `repeat` is left exactly as it was; `pg_task` sees `repeat > 0` and inserts the next occurrence as usual, so the task keeps retrying at that interval until it finally succeeds.

The one thing to avoid: don't wrap the real work in its own `EXCEPTION WHEN OTHERS` handler that swallows the error — that would make a failed run look successful to `pg_task` (and to the `UPDATE` that cancels `repeat`).

### Retry with exponential backoff

For a bounded number of retries with a growing delay between attempts (rather than "forever, at a fixed interval"), add your own bookkeeping columns and an `AFTER UPDATE` trigger that re-inserts a `FAIL`ed task with a computed `plan`:

```sql
ALTER TABLE task ADD COLUMN retry           int      NOT NULL DEFAULT 0;
ALTER TABLE task ADD COLUMN retry_max       int      NOT NULL DEFAULT 0;
ALTER TABLE task ADD COLUMN retry_interval  interval NOT NULL DEFAULT '1 min';

CREATE OR REPLACE FUNCTION task_retry() RETURNS trigger AS $f$
DECLARE
    columns text;
BEGIN
    IF NEW.retry >= NEW.retry_max THEN
        RETURN NEW;
    END IF;
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum) INTO columns
    FROM pg_attribute
    WHERE attrelid = TG_RELID AND attnum > 0 AND NOT attisdropped
      AND attname NOT IN ('id', 'plan', 'parent', 'start', 'stop', 'pid', 'state', 'error', 'output', 'retry');
    EXECUTE format(
        'INSERT INTO %I (parent, plan, retry, %s) SELECT id, statement_timestamp() + retry_interval * power(2, retry), retry + 1, %s FROM %I WHERE id = $1',
        TG_TABLE_NAME, columns, columns, TG_TABLE_NAME
    ) USING NEW.id;
    RETURN NEW;
END;
$f$ LANGUAGE plpgsql;

CREATE TRIGGER task_retry_trigger AFTER UPDATE OF state ON task
FOR EACH ROW WHEN (NEW.state = 'FAIL') EXECUTE FUNCTION task_retry();
```

The column list is looked up dynamically (the same trick `pg_task` itself uses in `task_columns()` to clone a row for `repeat`), so the trigger keeps working as you add more columns of your own. `parent` is set to the id of the attempt that just failed, so the whole retry chain stays visible through `parent`. `retry_interval * power(2, retry)` doubles the delay each attempt (`1 min`, `2 min`, `4 min`, ...); once `retry` reaches `retry_max` no new row is inserted and the last attempt is left at `FAIL`. `FAIL` rows are never auto-deleted by `pg_task.delete`, since that only fires when both `output` and `error` are null — a `FAIL` row always has `error` set — so the trigger always gets to see it.

### Conditional launch on a parent task ("on success" / "on failure")

`parent` (see Task table) records ancestry but by itself doesn't hold a child back from running — it's picked up as soon as its own `plan` is due, regardless of the parent. To actually gate a child on how its parent finished, without waiting on a brand-new `state` (self-provisioning only ever adds enum values it knows about, and the built-in `BEFORE UPDATE OF "state"` transition-validation trigger accepts no transition at all out of a state it doesn't recognize — you'd paint yourself into a corner trying to move a custom "waiting" state back to `PLAN`), hold the child in `PLAN` with `plan` pushed out to `infinity` instead, and only bring `plan` back down once the parent settles:

```sql
ALTER TABLE task ADD COLUMN depend text NOT NULL DEFAULT 'always' CHECK (depend IN ('success', 'failure', 'always'));

CREATE OR REPLACE FUNCTION task_depend_insert() RETURNS trigger AS $f$
DECLARE
    parent_state state;
BEGIN
    IF NEW.parent IS NULL THEN RETURN NEW; END IF;
    SELECT state INTO parent_state FROM task WHERE id = NEW.parent;
    IF parent_state IS NULL OR parent_state NOT IN ('DONE', 'FAIL') THEN
        NEW.plan := 'infinity';                  -- parent hasn't settled yet — wait
    ELSIF NOT ((parent_state = 'DONE' AND NEW.depend IN ('success', 'always'))
            OR (parent_state = 'FAIL' AND NEW.depend IN ('failure', 'always'))) THEN
        NEW.state := 'STOP';                      -- parent already settled the other way — never run
    END IF;
    RETURN NEW;
END;
$f$ LANGUAGE plpgsql;
CREATE TRIGGER task_depend_insert BEFORE INSERT ON task
FOR EACH ROW EXECUTE FUNCTION task_depend_insert();

CREATE OR REPLACE FUNCTION task_depend_update() RETURNS trigger AS $f$
BEGIN
    UPDATE task SET plan = statement_timestamp()
    WHERE parent = NEW.id AND state = 'PLAN' AND plan = 'infinity'
      AND ((NEW.state = 'DONE' AND depend IN ('success', 'always'))
        OR (NEW.state = 'FAIL' AND depend IN ('failure', 'always')));
    UPDATE task SET state = 'STOP'
    WHERE parent = NEW.id AND state = 'PLAN' AND plan = 'infinity'
      AND NOT ((NEW.state = 'DONE' AND depend IN ('success', 'always'))
            OR (NEW.state = 'FAIL' AND depend IN ('failure', 'always')));
    RETURN NEW;
END;
$f$ LANGUAGE plpgsql;
CREATE TRIGGER task_depend_update AFTER UPDATE OF state ON task
FOR EACH ROW WHEN (NEW.state IN ('DONE', 'FAIL')) EXECUTE FUNCTION task_depend_update();
```

This relies on two more of `pg_task`'s own self-provisioned triggers, beyond the three named in Self-provisioning and the helper triggers above: `plan` is only frozen *after* a task leaves `PLAN` (`BEFORE UPDATE OF "plan"`, conditional on `OLD.state <> 'PLAN'`), so a still-`PLAN` child's `plan` remains freely updatable — and updating it fires the existing wake-up trigger for free, so a released child is picked up immediately rather than on the next poll. `plan = 'infinity'` also keeps the child out of reach of the `active`-window `GONE` transition (`plan + active` stays `infinity` too), so it can wait indefinitely without expiring. Moving an unsatisfied child straight to `STOP` is the one transition the built-in state-machine trigger allows out of `PLAN` besides `TAKE`/`GONE`.

This only expresses a single upstream dependency (one `depend` per `parent`, matching the column's own cardinality), not a multi-parent join.

### Cron-like scheduling

`repeat` is a fixed `interval` — it can't express "every weekday at 9am" or "the 1st of the month". Rather than a fixed step, compute the next occurrence yourself from a 5-field cron expression (`min hour dom month dow`) and drive `plan` with it directly, leaving `repeat` at its default `0 sec` so the built-in auto-repeat stays out of the way:

```sql
CREATE OR REPLACE FUNCTION cron_field(spec text, lo int, hi int) RETURNS int[] AS $f$
DECLARE
    part text; rng text; step int; a int; b int; result int[] := '{}';
BEGIN
    FOREACH part IN ARRAY string_to_array(spec, ',') LOOP
        IF part LIKE '%/%' THEN
            rng := split_part(part, '/', 1); step := split_part(part, '/', 2)::int;
        ELSE
            rng := part; step := 1;
        END IF;
        IF rng = '*' THEN a := lo; b := hi;
        ELSIF rng LIKE '%-%' THEN a := split_part(rng, '-', 1)::int; b := split_part(rng, '-', 2)::int;
        ELSE a := rng::int; b := a;
        END IF;
        IF a < lo OR b > hi THEN RAISE EXCEPTION 'cron field value out of range [%,%]: %', lo, hi, part; END IF;
        SELECT result || array_agg(x) FROM generate_series(a, b, step) x INTO result;
    END LOOP;
    RETURN ARRAY(SELECT DISTINCT x FROM unnest(result) x ORDER BY x);
END;
$f$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION cron_next(expr text, from_ts timestamptz DEFAULT now()) RETURNS timestamptz AS $f$
DECLARE
    fields text[] := regexp_split_to_array(btrim(expr), '\s+');
    min_set int[]; hour_set int[]; dom_set int[]; mon_set int[]; dow_set int[];
    dom_restricted boolean; dow_restricted boolean;
    d date; h int; mi int; day_ok boolean;
BEGIN
    IF array_length(fields, 1) <> 5 THEN
        RAISE EXCEPTION 'cron expression must have 5 fields (min hour dom month dow): %', expr;
    END IF;
    min_set  := cron_field(fields[1], 0, 59);
    hour_set := cron_field(fields[2], 0, 23);
    dom_set  := cron_field(fields[3], 1, 31);
    mon_set  := cron_field(fields[4], 1, 12);
    dow_set  := cron_field(fields[5], 0, 6);
    dom_restricted := fields[3] <> '*';
    dow_restricted := fields[5] <> '*';
    d := date_trunc('minute', from_ts)::date;
    FOR i IN 0 .. 1465 LOOP -- ~4 years, enough to always catch a Feb-29-only schedule
        day_ok := mon_set @> ARRAY[EXTRACT(month FROM d)::int] AND (
            CASE WHEN dom_restricted AND dow_restricted -- vixie-cron semantics: dom/dow combine with OR when both are restricted
                 THEN dom_set @> ARRAY[EXTRACT(day FROM d)::int] OR dow_set @> ARRAY[EXTRACT(dow FROM d)::int]
                 ELSE dom_set @> ARRAY[EXTRACT(day FROM d)::int] AND dow_set @> ARRAY[EXTRACT(dow FROM d)::int]
            END);
        IF day_ok THEN
            FOREACH h IN ARRAY hour_set LOOP
                FOREACH mi IN ARRAY min_set LOOP
                    IF (d + h * interval '1 hour' + mi * interval '1 min') > from_ts THEN
                        RETURN d + h * interval '1 hour' + mi * interval '1 min';
                    END IF;
                END LOOP;
            END LOOP;
        END IF;
        d := d + 1;
    END LOOP;
    RAISE EXCEPTION 'no matching time found for cron expression % within search horizon', expr;
END;
$f$ LANGUAGE plpgsql STABLE;

ALTER TABLE task ADD COLUMN cron text; -- NULL for ordinary, non-cron tasks

CREATE OR REPLACE FUNCTION task_cron() RETURNS trigger AS $f$
DECLARE
    columns text;
BEGIN
    IF NEW.cron IS NULL THEN RETURN NEW; END IF;
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum) INTO columns
    FROM pg_attribute
    WHERE attrelid = TG_RELID AND attnum > 0 AND NOT attisdropped
      AND attname NOT IN ('id', 'plan', 'parent', 'start', 'stop', 'pid', 'state', 'error', 'output');
    EXECUTE format(
        'INSERT INTO %I (parent, plan, %s) SELECT id, cron_next(cron, statement_timestamp()), %s FROM %I WHERE id = $1',
        TG_TABLE_NAME, columns, columns, TG_TABLE_NAME
    ) USING NEW.id;
    RETURN NEW;
END;
$f$ LANGUAGE plpgsql;

CREATE TRIGGER task_cron_trigger AFTER UPDATE OF state ON task
FOR EACH ROW WHEN (NEW.state IN ('DONE', 'FAIL')) EXECUTE FUNCTION task_cron();
```

`cron_field()` expands one field (`*`, `a-b`, `*/n`, `a-b/n`, comma-separated lists of any of those) into a sorted array of matching values; `cron_next()` walks forward day by day (bounded to roughly four years, long enough to land on a Feb-29-only schedule) and, on a day that matches month/day-of-month/day-of-week, returns the first hour:minute in the field sets that's after `from_ts`. Both are ordinary `IMMUTABLE`/`STABLE` SQL functions, so you can also call `cron_next()` directly to sanity-check an expression before using it. `task_cron()` reuses the same dynamic column-clone trick as the retry trigger above; only rows with `cron IS NOT NULL` are affected, so it's safe to add alongside tasks that use plain `repeat`.

This parser only covers the classic 5-field syntax (numeric fields, `*`, ranges, steps, lists) — no named days/months (`MON`, `JAN`), no `L`/`W`/`#`, no seconds field. Extend `cron_field()`/`cron_next()` if you need those.

### Notifications on completion

Rather than polling `task`, have it push: `pg_notify()` for listeners inside the same database, or a webhook (via the [`pg_curl`](https://github.com/RekGRpth/pg_curl) extension) for anything outside it — both as a plain `AFTER UPDATE OF state` trigger, no core changes needed.

`NOTIFY`, no extra dependencies:

```sql
CREATE OR REPLACE FUNCTION task_notify() RETURNS trigger AS $f$
BEGIN
    PERFORM pg_notify('task_done', json_build_object(
        'id', NEW.id, 'group', NEW."group", 'state', NEW.state,
        'output', NEW.output, 'error', NEW.error
    )::text);
    RETURN NEW;
END;
$f$ LANGUAGE plpgsql;

CREATE TRIGGER task_notify_trigger AFTER UPDATE OF state ON task
FOR EACH ROW WHEN (NEW.state IN ('DONE', 'FAIL')) EXECUTE FUNCTION task_notify();
```

Webhook, opt-in per row via its own `hook` column (`NULL` — no call), via `pg_curl`:

```sql
ALTER TABLE task ADD COLUMN hook text;

CREATE EXTENSION IF NOT EXISTS pg_curl;

CREATE OR REPLACE FUNCTION task_hook() RETURNS trigger AS $f$
BEGIN
    IF NEW.hook IS NULL THEN RETURN NEW; END IF;
    BEGIN
        PERFORM curl_easy_reset();
        PERFORM curl_easy_setopt_url(NEW.hook);
        PERFORM curl_easy_setopt_post(1);
        PERFORM curl_header_append('Content-Type', 'application/json');
        PERFORM curl_easy_setopt_postfields(convert_to(json_build_object(
            'id', NEW.id, 'group', NEW."group", 'state', NEW.state,
            'output', NEW.output, 'error', NEW.error
        )::text, 'UTF8'));
        PERFORM curl_easy_perform(timeout_ms => 5000);
    EXCEPTION WHEN OTHERS THEN NULL; -- a broken/slow endpoint must not fail the task itself
    END;
    RETURN NEW;
END;
$f$ LANGUAGE plpgsql;

CREATE TRIGGER task_hook_trigger AFTER UPDATE OF state ON task
FOR EACH ROW WHEN (NEW.state IN ('DONE', 'FAIL')) EXECUTE FUNCTION task_hook();
```

Tested against a real local listener: both `DONE` and `FAIL` rows produced a correctly-formed JSON POST. The one thing to get right: this trigger runs synchronously, inside the same transaction that `task_done()`/`task_error()` (`task.c`) use to record the result — a webhook call that hangs or errors would otherwise hang or fail that write too, which is why the call is wrapped in its own `EXCEPTION WHEN OTHERS` and given an explicit `timeout_ms` rather than left to block indefinitely.

### History/archive instead of a bare delete

Rather than `pg_task.delete` sending finished rows into nowhere, copy them into a partitioned archive table on the way out — an `AFTER DELETE` trigger, so it uniformly covers both `task_delete()`'s own auto-delete (`delete = true` with `output`/`error` both null) and any periodic cleanup you run yourself (e.g. a repeating `pg_task` job whose `input` is `DELETE FROM task WHERE state IN ('DONE', 'GONE') AND plan < now() - interval '7 days'`):

```sql
CREATE TABLE task_archive (LIKE task INCLUDING DEFAULTS, PRIMARY KEY (id, stop))
    PARTITION BY RANGE (stop);
CREATE TABLE task_archive_default PARTITION OF task_archive DEFAULT;
-- plus dated partitions, by hand or on a schedule -- see Automatic partitioning below

CREATE OR REPLACE FUNCTION task_archive() RETURNS trigger AS $f$
BEGIN
    BEGIN
        INSERT INTO task_archive SELECT OLD.*;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'task_archive: failed to archive id = %: %', OLD.id, SQLERRM;
    END;
    RETURN OLD;
END;
$f$ LANGUAGE plpgsql;

CREATE TRIGGER task_archive_trigger AFTER DELETE ON task
FOR EACH ROW EXECUTE FUNCTION task_archive();
```

Two things confirmed by testing this end to end: a partitioned table's unique/primary key must include the partition key, so this is `PRIMARY KEY (id, stop)`, not just `(id)` — `LIKE task INCLUDING ALL` would carry over `task`'s own `(id)` primary key and fail with `PRIMARY KEY constraint ... must include all partitioning columns`. And the `DEFAULT` partition and the `EXCEPTION WHEN OTHERS` are both load-bearing, not redundant: like the webhook trigger above, this one runs inside the same transaction `task_done()`/`task_error()` use to delete the row, so an archive insert failing outright (no partition covers this `stop`, some constraint, ...) would otherwise take that deletion — and the task's own bookkeeping — down with it. With the `DEFAULT` partition in place a row with no dedicated partition still lands somewhere; with neither, the deletion still succeeds and only a `WARNING` is logged, verified by deliberately deleting both safety nets and confirming the row failed to archive but `DELETE FROM task` still returned normally.

### Automatic partitioning via a repeating task

Rather than reaching for an extension like `pg_partman` to keep a partitioned table (e.g. `task_archive` above) supplied with future partitions, `pg_task` can do it itself — a repeating task is exactly a scheduler already living inside the database:

```sql
CREATE OR REPLACE FUNCTION task_archive_partition(lead interval DEFAULT '1 month') RETURNS void AS $f$
DECLARE
    from_ts timestamptz := date_trunc('month', now());
    to_ts timestamptz := date_trunc('month', now() + lead) + '1 month';
BEGIN
    WHILE from_ts < to_ts LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF task_archive FOR VALUES FROM (%L) TO (%L)',
            'task_archive_' || to_char(from_ts, 'YYYY_MM'), from_ts, from_ts + '1 month'
        );
        from_ts := from_ts + '1 month';
    END LOOP;
END;
$f$ LANGUAGE plpgsql;

INSERT INTO task (repeat, input) VALUES ('1 day', $$SELECT task_archive_partition()$$);
```

The repeating task keeps one month of lead time stocked up ahead of the current date; `CREATE TABLE IF NOT EXISTS` makes each daily run a no-op once that month's partition already exists, so a missed or doubled-up firing is harmless. Widen `lead` or shrink the `repeat` interval for finer-grained (weekly/daily) partitioning.

### Structured tags/metadata

`data` already exists for free-form user data, but filtering or grouping tasks by it means parsing whatever ad hoc format ended up in there. For external tooling (monitoring, orchestration, multi-tenant setups) that wants to query tasks by arbitrary, evolving labels without a schema migration for every new one, a `jsonb` column with a GIN index does the job — plain DDL, `pg_task` never touches a column it didn't create itself:

```sql
ALTER TABLE task ADD COLUMN meta jsonb NOT NULL DEFAULT '{}';
CREATE INDEX task_meta_gin ON task USING gin (meta jsonb_path_ops);
```

```sql
INSERT INTO task (input, meta) VALUES ($$SELECT 1$$, '{"tenant": "acme", "job_type": "report"}');

SELECT * FROM task WHERE meta @> '{"tenant": "acme"}';
```

No trigger needed — `meta` is just carried along like any other column you add, `jsonb_path_ops` keeps the index small and fast for the `@>` containment queries this is normally used for. Reach for a proper typed column instead if you find yourself always filtering on the same one or two keys; `jsonb` earns its keep for labels whose shape you don't want to commit to up front.

### Idempotency key

Nothing stops the same logical job from being inserted twice — a retrying caller, an at-least-once event delivery, a cron trigger firing twice on a clock skew. A unique key, checked while the row is still "live", turns a duplicate `INSERT` into a no-op instead of a duplicate run:

```sql
ALTER TABLE task ADD COLUMN idem_key text;

CREATE UNIQUE INDEX task_idem_key_uq ON task (idem_key)
    WHERE idem_key IS NOT NULL AND state IN ('PLAN', 'TAKE', 'WORK');

INSERT INTO task (input, idem_key) VALUES ($$...$$, 'daily-report-2026-09-17')
    ON CONFLICT (idem_key) WHERE idem_key IS NOT NULL AND state IN ('PLAN', 'TAKE', 'WORK') DO NOTHING;
```

The index has to be partial, and specifically scoped to `PLAN`/`TAKE`/`WORK`, not unconditional: a task that already reached `DONE`/`FAIL`/`GONE`/`STOP` has settled, so the same `idem_key` should be insertable again for the next occurrence of that logical job. An unconditional unique index would instead permanently block any future insert of that key the moment the first one finishes.

### OpenTelemetry trace propagation

To see a chain of `parent`/child tasks as a single trace in Jaeger/Tempo/etc. rather than as unrelated rows, carry a trace context through the same `parent` link `pg_task` already tracks:

```sql
ALTER TABLE task ADD COLUMN trace_id text;
ALTER TABLE task ADD COLUMN span_id text;

CREATE OR REPLACE FUNCTION task_trace_insert() RETURNS trigger AS $f$
BEGIN
    IF NEW.parent IS NOT NULL AND NEW.trace_id IS NULL THEN
        SELECT trace_id INTO NEW.trace_id FROM task WHERE id = NEW.parent;
    END IF;
    IF NEW.trace_id IS NOT NULL THEN
        NEW.span_id := encode(gen_random_bytes(8), 'hex');
    END IF;
    RETURN NEW;
END;
$f$ LANGUAGE plpgsql;

CREATE TRIGGER task_trace_insert_trigger BEFORE INSERT ON task
FOR EACH ROW EXECUTE FUNCTION task_trace_insert();
```

This only populates the columns; actually exporting them as OTLP spans needs a process outside `pg_task` itself, since none of the three execution paths (see Three ways to run `input`) can make an arbitrary HTTP/gRPC call — `remote` only speaks the PostgreSQL protocol. A periodic `pg_task` job (or an external poller) reading newly-`DONE`/`FAIL` rows and pushing them to a collector, e.g. via [`pg_curl`](https://github.com/RekGRpth/pg_curl) against an OTLP/HTTP endpoint the same way the webhook pattern above posts JSON, is one way to close that gap.

### Dry-run validation

Before letting a batch of externally-generated tasks anywhere near the real queue, validate `input` without letting its effects stick — actually run it, then unconditionally roll back, inside a `BEFORE INSERT` trigger:

```sql
ALTER TABLE task ADD COLUMN dry_run bool NOT NULL DEFAULT false;

CREATE OR REPLACE FUNCTION task_dry_run() RETURNS trigger AS $f$
BEGIN
    IF NOT NEW.dry_run THEN RETURN NEW; END IF;
    BEGIN
        EXECUTE NEW.input;
        RAISE EXCEPTION 'task_dry_run: rollback sentinel';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLERRM <> 'task_dry_run: rollback sentinel' THEN
                NEW.error := SQLERRM;
            END IF;
    END;
    NEW.state := 'STOP';
    RETURN NEW;
END;
$f$ LANGUAGE plpgsql;

CREATE TRIGGER task_dry_run_trigger BEFORE INSERT ON task
FOR EACH ROW WHEN (NEW.dry_run) EXECUTE FUNCTION task_dry_run();
```

The `BEGIN ... EXCEPTION ... END` block is an implicit savepoint, so raising the sentinel exception right after `EXECUTE` unconditionally discards whatever `input` just did, real side effects included — this is closer to actually validating the statement than `EXPLAIN` would be (which rejects DDL and most utility statements outright), at the cost of genuinely executing `input` once. This does not reproduce all three execution paths equally: PL/pgSQL's `EXECUTE` runs through SPI regardless of the task's own `spi` setting, so a `dry_run` task using `COPY ... TO/FROM` a file or program — allowed for a real `spi = off` run — is rejected here (`SPI_ERROR_COPY`) even though it would have succeeded for real. Treat a clean dry run as "no syntax/permission/missing-object errors caught this way", not as a full guarantee.

## Capabilities and limitations

Capabilities:
- run arbitrary SQL as soon as possible, at a specific time (`plan`), or repeatedly every N (`repeat`, counted from the original `plan` or from the actual finish time via `drift`);
- catch execution errors without taking down a worker — the error text goes to `error`, the result to `output`;
- run tasks concurrently with per-group concurrency control (`group` + `max`), or, with a negative `max`, pace them with a fixed delay instead — a built-in rate limiter;
- run `input` on the local database or on an arbitrary remote one (`remote`), over a non-blocking connection that doesn't block the scheduler from polling everything else;
- run in several databases, schemas, tables and as several roles at once, via `pg_task.json`;
- for non-`remote` tasks, run `input` either through the same dispatcher a real client connection uses (full DDL/`COPY`/multi-statement, `spi = off`) or through SPI (`spi = on`, faster and narrower) — see Three ways to run `input` above for how `remote` tasks differ from both;
- reuse one session across several consecutive tasks of a group (`pg_task.save`), keeping temp tables/prepared statements/settings between runs;
- bound a task's own run time (`timeout`) independently of how long the executing process itself lives (`live`, `count`);
- react to a new or changed task almost immediately rather than only on the next poll, thanks to the advisory-lock/`pg_cancel_backend` wake-up described in Architecture above;
- cancel a running task from SQL (`UPDATE task SET state = 'STOP' WHERE id = ...`), local or remote, leaving it distinguishably at `STOP` rather than `FAIL`, and without scheduling its next `repeat` occurrence — see [Task state machine](#task-state-machine);
- recover from a worker crashing mid-task — orphaned `TAKE`/`WORK` rows are reset to `PLAN` every `pg_task.reset`;
- format results (delimiter, quoting, escaping, `NULL` representation, headers);
- run every task as the role that actually inserted it, not as the scheduler's own connecting role (see Security considerations below).

GUCs, and most `task` columns, cascade through up to six levels — the config file, then per-database, per-role, per-role-in-database and per-session overrides, and finally the value on the row itself — the most specific one wins.

Limitations:
- if the executor crashes in the narrow window between `STOP` being set and the cancellation actually completing, the row can be left at `STOP` with `stop`/`error` never filled in — `work_reset` deliberately leaves `STOP` rows alone (unlike `TAKE`/`WORK`) so a cancelled task isn't accidentally resurrected back to `PLAN`;
- no built-in dashboard or UI — monitoring is `SELECT * FROM task` and, as needed, `pg_locks`/`pg_stat_activity`;
- `input` is indexed (btree), so an `input` still larger than about 2.7 kB once compressed can't be inserted (`index row size ... exceeds btree version 4 maximum 2704`); how far it compresses depends on `default_toast_compression` (lz4 packs repetitive text much tighter than pglz);
- tasks only run where the table can be written to — a read-only replica can't run a scheduler against it;
- with `log_statement = all` and the like, the queries of a task's bookkeeping are logged with their parameters, the task's `output` and `error` among them, cut, at a character, by `log_parameter_max_length` (PostgreSQL 13+) as the server's own are, and to 64 MB all together in any case;
- concurrency is counted per group by a 32-bit hash of `"group" || COALESCE("remote", '')` (the advisory locks that hold the slots of a group carry nothing longer), so two groups of one table whose hashes collide share their `max` — one waits while the other's tasks run; nothing else is shared, a worker taking on the next task of its group and a pause pushing back the next tasks compare `group` and `remote` themselves. By chance that takes some thousands of groups in a table (about 1% for 10,000), and by the concatenation a local group whose name ends with another group's `remote` (`'xdbname=foo'` and `'x'` with `remote = 'dbname=foo'`); to look for it, `SELECT hashtext("group" || COALESCE(remote, '')) AS hash, array_agg(DISTINCT ("group", remote)) FROM task GROUP BY 1 HAVING count(DISTINCT ("group", remote)) > 1`;
- no coordination across independent clusters — if the same `task` table or the same `remote` target is reachable from more than one place at once, consistency is on you; the advisory locks only protect against races inside one server instance;
- building requires network access to GitHub, or a pre-existing source tree (see Build above) — the extension pulls part of the matching version's real `postgres.c` at build time rather than just linking against an installed server;
- only the server branches/versions actually exercised by CI are supported — a brand-new major version may need a follow-up patch before the code extracted from its `postgres.c` matches again;
- a task has exactly the privileges of its role, in local and SPI mode alike, including `COPY ... TO/FROM PROGRAM` if the role can do that — this follows directly from running `input` as that role, not from a bug; if task authors aren't fully trusted, restrict the role's privileges (see Security considerations below).

## Security considerations

`pg_task` executes the raw SQL text stored in `task.input` as the role recorded in `task.user`. That column defaults to `current_user` at insert time, is overwritten to `current_user` by a `BEFORE INSERT` trigger unless the inserting role is a member of the role it names (or of the table's owner, `pg_task.user`) — on PostgreSQL 16+ one that may `SET ROLE` to it, not a membership `WITH SET FALSE`, which counts for neither this trigger, the row level security policy nor the state machine — and is immutable afterwards (a `BEFORE UPDATE OF "user"` trigger rejects any change). On top of that, `pg_work` refuses to run a local task as a role `pg_task.user` couldn't `SET ROLE` to (see below). Since `task.user` is thus trustworthy, the background worker that runs a local task connects to the database **as `task.user` itself** — its `session_user` is the task author, not `pg_task.user` (default: `postgres`). Only the worker's own housekeeping queries (marking the task `WORK`, recording `state`/`output`/`error`, scheduling repeats, etc.) switch to `pg_task.user`, the same way a `SECURITY DEFINER` function does, only inside their own transaction and as a security-restricted operation (no temporary objects, `PREPARE`, `LISTEN`, `SET ROLE` and the like meanwhile), so that nothing the author's session sets up can take part in them, and with an empty `search_path`. Keep that in mind for your own triggers on the task table: they fire within those queries too, so they should qualify the names they use or set a `search_path` of their own (`CREATE FUNCTION ... SET search_path = ...`). A worker kept alive by `count`/`live` only picks up further tasks of the same author.

This means **a task only ever runs with the privileges of whoever actually inserted it**: `RESET ROLE`, `SET ROLE` or `SET SESSION AUTHORIZATION` inside `input` can at most get back to `task.user`, never to `pg_task.user`.

Things to keep in mind:

- **`pg_task.user` must be allowed to act as the task author**: be a superuser, or be able to `SET ROLE` to the author (plain membership, or on PostgreSQL 16+ membership `WITH SET TRUE`). Grant it only the roles it should run tasks as: `GRANT author_role TO pg_task_user WITH INHERIT FALSE, SET TRUE;`. `pg_work` checks this before starting a local task and fails it otherwise, since `pg_task.user` owns the task table and so is not bound by the `user` column's triggers itself. The same membership keeps the author on the repeats of a task, which `pg_task.user` inserts.
- **The task author must be able to connect**: `CONNECT` on the database, and, before PostgreSQL 17, `LOGIN` (on 17+ the worker bypasses the `LOGIN` check, so `NOLOGIN` group roles still work). Otherwise the task ends up `FAIL` with the corresponding error instead of being run. Per-role settings (`ALTER ROLE author_role SET ...`) apply to the task session just like to a normal login of that role.
- **Row level security (PostgreSQL 9.5+) keeps authors apart.** The self-provisioned policy lets a role see, update and delete only the tasks whose `user` it is, or is a member of, and lets a member of the table's owner (`pg_task.user`) act on every task — the same rule the `user` trigger applies on insert. Without it, anyone with `UPDATE` on `task` could rewrite the `input` of someone else's still-queued task and have it run as that someone, read other authors' `output`/`error` and the passwords in their `remote`, or `STOP`/delete their tasks. Row level security is enabled but not forced, so the table owner (`pg_task.user`) — and with it `pg_work` and the task's bookkeeping — still sees every task, as do superusers and `BYPASSRLS` roles; a role that should monitor all tasks needs one of those, or an extra permissive policy of your own (`CREATE POLICY ... USING (true)`, which is OR'ed with the built-in one). `pg_work` re-creates the policy and re-enables row level security if either goes missing, and rewrites the policy whenever its comment, where `pg_work` records the policy's expression, no longer matches the expression of this version of `pg_task` — so to widen access add a policy of your own, as above, rather than altering this one. On PostgreSQL 9.4 there is none of this, so there grant `SELECT`/`UPDATE`/`DELETE` on `task` only to roles you trust with every author's tasks.
- **The schema, the task table, its `state` type and its trigger functions must be `pg_task`'s own.** `pg_work` refuses to take in any of them that someone else made before it did and still owns — as anyone could in `public` before PostgreSQL 15, which would leave them theirs to change, and the policy with the table, and the body of a trigger function, which `CREATE OR REPLACE` keeps theirs — unless it's owned by `pg_task.user` or a superuser: it stops with `... exists and is owned by ..., not by pg_task.user or a superuser`. Have such an object owned by `pg_task.user` (`ALTER ... OWNER TO`) if it's to be trusted with the tasks of others, or drop it; the same goes for objects made by an earlier `pg_task.user`, when changing it to another role than a superuser. The schema is checked too, as its owner may drop the objects of others in it, the task table say, for one of their own, whose triggers would then run as `pg_task.user`: from PostgreSQL 15 on `public` is owned by `pg_database_owner`, that is by the owner of the database, so a database owned by a role other than `pg_task.user` or a superuser keeps `pg_work` out of its `public` schema; use a schema of `pg_task.user`'s own there.
- **Restrict who can `INSERT` into `task`** (`REVOKE INSERT ON task FROM PUBLIC` and grant it selectively, or only allow inserts through a `SECURITY DEFINER` wrapper function). The `user` column protects against forged *identity*, not against a task simply running with its author's genuinely-granted privileges.
- **`input` can do anything its effective role (`task.user`) is allowed to**, with `pg_task.spi` off (the default, a normal top-level statement) and on alike — including `COPY ... TO`/`FROM` a server-side file or program if that role has the required privilege: SPI rejects only `COPY ... FROM STDIN`/`TO STDOUT`, so `pg_task.spi = on` is no safeguard against it. Restrict the role's privileges instead (`pg_read_server_files`, `pg_write_server_files`, `pg_execute_server_program`, superuser).
- **The `remote` column's password requirement reflects the actual task author.** Unless `task.user` is a superuser, `pg_task` refuses to connect when the `remote` connection string has no password, or an empty one, which libpq would look up in the password file of the server's own OS user instead (`work.c`'s `work_remote`), and fails the task once connected if the remote server didn't actually ask for the password (`work_connect`), the same way `dblink` does. Once connected, execution on the remote server is governed entirely by the credentials in the `remote` connection string — `task.user`/`SET ROLE` have no effect there, since the remote server has its own, independent role catalog. Don't put passwordless/trust-authenticated connection strings within reach of task authors you don't fully trust.
- Review which extensions/functions any role granted to `pg_task.user` can execute (`dblink`, `adminpack`, large-object functions, `pg_read_file`, etc.) and revoke what isn't needed.
- Consider auditing (e.g. `pgaudit`) or alerting on `task.input` containing sensitive constructs (`COPY ... PROGRAM`, `CREATE FUNCTION ... LANGUAGE c`, `ALTER SYSTEM`, `dblink`, ...) if task authorship is not fully trusted.
