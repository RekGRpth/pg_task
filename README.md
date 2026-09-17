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
| pg_task.spi | bool | false | config, database, user, session | SPI (or local) execution? Also affects `input` containing multiple `;`-separated statements: on PostgreSQL 10+, SPI mode runs each statement separately and appends all results, same as local mode; on pre-10 backends, SPI mode returns only the last statement's result |
| pg_task.string | bool | true | config, database, user, session | Quote only strings |
| pg_conf.fetch | int | 10 | config, database, superuser | Fetch conf rows at once |
| pg_conf.max | int | max_worker_processes | config | Maximum task and work workers |
| pg_conf.restart | int | 60 | config, database, superuser | Restart conf interval, seconds |
| pg_task.count | int | 0 | config, database, user, session | Non-negative maximum count of tasks, are executed by current background worker process before exit |
| pg_task.fetch | int | 100 | config, database, user | Fetch task rows at once |
| pg_task.id | bigint | 0 | session | Current task id (for read only) |
| pg_task.limit | int | 1000 | config, database, user | Limit task rows at once |
| pg_task.max | int | 0 | config, database, user, session | Maximum count of additional concurrently executing tasks in group (total concurrency = max + 1), negative value means pause between tasks in milliseconds |
| pg_task.run | int | 2147483647 | config, database, user, session | Maximum count of concurrently executing tasks in work |
| pg_task.sleep | int | 1000 | config, database, user | Check tasks every sleep milliseconds |
| pg_work.fetch | int | 100 | config, database, superuser | Fetch work rows at once |
| pg_work.idle | int | 60 | config, database, user | Idle work count |
| pg_work.restart | int | 60 | config, database, superuser | Restart work interval, seconds |
| pg_task.active | interval | 1 hour | config, database, user, session | Positive period after plan time, when task is active for executing |
| pg_task.data | text | postgres | config | Database name for tasks table |
| pg_task.delimiter | char | \t | config, database, user, session | Results columns delimiter |
| pg_task.escape | char | | config, database, user, session | Results columns escape |
| pg_task.group | text | group | config, database, user, session | Task grouping by name |
| pg_task.json | json | [{"data":"postgres"}] | config | Json configuration, available keys: data, reset, schema, table, sleep and user |
| pg_task.live | interval | 0 sec | config, database, user, session | Non-negative maximum time of live of current background worker process before exit |
| pg_task.null | text | \N | config, database, user, session | Null text value representation |
| pg_task.plan | timestamptz | statement_timestamp() | config, database, user, session | Default value for plan timestamp |
| pg_task.quote | char | | config, database, user, session | Results columns quote |
| pg_task.repeat | interval | 0 sec | config, database, user, session | Non-negative auto repeat tasks interval |
| pg_task.reset | interval | 1 hour | config, database, user | Interval of reset tasks |
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
| plan | timestamptz | NOT NULL | pg_task.plan | Planned date and time of start |
| start | timestamptz | NULL | | Actual date and time of start |
| stop | timestamptz | NULL | | Actual date and time of stop |
| active | interval | NOT NULL | pg_task.active | Positive period after plan time, when task is active for executing |
| live | interval | NOT NULL | pg_task.live | Non-negative maximum time of live of current background worker process before exit |
| repeat | interval | NOT NULL | pg_task.repeat | Non-negative auto repeat tasks interval |
| timeout | interval | NOT NULL | pg_task.timeout | Non-negative allowed time for task run |
| count | int | NOT NULL | pg_task.count | Non-negative maximum count of tasks, are executed by current background worker process before exit |
| max | int | NOT NULL | pg_task.max | Maximum count of additional concurrently executing tasks in group (total concurrency = max + 1), negative value means pause between tasks in milliseconds |
| pid | int | NULL | | Id of process executing task |
| state | enum state (PLAN, GONE, TAKE, WORK, DONE, FAIL, STOP) | NOT NULL | PLAN | Task state |
| delete | bool | NOT NULL | pg_task.delete | Auto delete task when both output and error are nulls |
| drift | bool | NOT NULL | pg_task.drift | Compute next repeat time by stop time instead by plan time |
| header | bool | NOT NULL | pg_task.header | Show columns headers in output (only when the query returns at least one row and more than one column) |
| save | bool | NOT NULL | pg_task.save | Save session state between tasks |
| string | bool | NOT NULL | pg_task.string | Quote only strings |
| delimiter | char | NOT NULL | pg_task.delimiter | Results columns delimiter |
| escape | char | NOT NULL | pg_task.escape | Results columns escape |
| quote | char | NOT NULL | pg_task.quote | Results columns quote |
| data | text | NULL | | Some user data |
| error | text | NULL | | Catched error |
| group | text | NOT NULL | pg_task.group | Task grouping by name |
| input | text | NOT NULL | | Sql command(s) to execute |
| null | text | NOT NULL | pg_task.null | Null text value representation |
| output | text | NULL | | Received result(s) |
| remote | text | NULL | | Connect to remote database (if need) |
| user | name | NOT NULL | current_user | Role that inserted the task; input is executed as this role, and the column is immutable after insert |

You may freely add your own columns to `task` and/or partition it — `pg_task` only ever touches the columns it created (see [Self-provisioning and the helper triggers](#self-provisioning-and-the-helper-triggers)).

## Running in multiple databases

By default `pg_task` runs a single scheduler, on the default database (`postgres`), as the default user (`postgres`), watching the default schema (`public`) and table (`task`), polling every default `sleep` interval.

To run more than one scheduler — e.g. one per application database, each with its own user/schema/table/poll interval — list them in `pg_task.json`, one object per scheduler; any key you omit falls back to its GUC default:
```conf
pg_task.json = '[{"data":"database1"},{"data":"database2","user":"username2"},{"data":"database3","schema":"schema3"},{"data":"database4","table":"table4"},{"data":"database5","sleep":100}]'
```
`pg_task` creates whichever of the referenced database, user, schema or table don't already exist — you don't need to provision them by hand first.

## Architecture

`pg_task` has no `pg_task--<version>.sql` control script and is never activated with `CREATE EXTENSION` — everything it needs (role, database, schema, table, the `state` enum, indexes, defaults, constraints and two helper triggers) is created idempotently by the extension itself the first time it starts, by checking `pg_catalog` before every `CREATE`/`ALTER` and only touching what's missing or mismatched. Rerunning it, or adding your own columns/partitions by hand, is safe — `pg_task` never drops or rewrites what it didn't create itself.

### Process hierarchy

The extension is split into four parts, each backed by its own source file and, except the first, its own background worker type:

1. **`init`** registers the extension's GUCs and, once, the single static background worker `pg_conf` (started under `shared_preload_libraries`).
2. **`pg_conf`** (one process per postmaster; on Green(plum|gage), coordinator only) parses `pg_task.json` — together with any per-database/per-role overrides — every `pg_conf.restart` seconds, creates the referenced role/database if missing, and launches one dynamic background worker `pg_work` per `{data, schema, table, user, sleep, ...}` entry.
3. **`pg_work`** (one process per such entry) is the scheduler proper: it provisions the schema/table on first connect (see below), then loops on a wait-event set — its latch plus the sockets of any open remote connections — periodically claiming due rows from the task table (respecting `plan`, `active` and per-group concurrency), expiring overdue non-repeating rows to `GONE`, and resetting rows orphaned by a crashed worker back to `PLAN`. Local tasks (no `remote`) are handed off to a child `pg_task` worker; remote tasks are driven directly by `pg_work` over an async, non-blocking `libpq` connection — no extra OS process per remote task.
4. **`pg_task`** (one process per concurrently running local task) connects, executes `input` (locally or via SPI, see below), writes `output`/`error`, flips `state` to `DONE`/`FAIL`, schedules the next `repeat` occurrence if any, and exits — or, within `count`/`live`, picks up another task of the same group before exiting.

Parameters are passed down the hierarchy through dynamic shared memory allocated at worker startup, not through command-line arguments or files.

### Task state machine

`PLAN → TAKE → WORK → DONE | FAIL`, or `PLAN → GONE` when a non-repeating task's `active` window elapses before it's picked up (typically: the server was down longer than `active` allows). `STOP` is a manual, terminal state you set yourself (`UPDATE task SET state = 'STOP' WHERE id = ...`); no code transitions a row into it automatically. Setting it on a `PLAN` row just keeps it from ever being claimed (it's filtered out by `state = 'PLAN'` like any other non-`PLAN` row). Setting it on a `WORK` row actually cancels the running task: a trigger cancels the local backend directly, or, for a `remote` task, wakes the owning `pg_work` (the same advisory-lock mechanism as the wake-up trigger below) so it calls `PQcancel()` on that connection. Either way the cancelled query's error is caught the normal way, but the row is left at `STOP` instead of being overwritten to `FAIL`, and — unlike a plain `FAIL` — its next `repeat` occurrence, if any, is not inserted. There's still no way to cancel a task from SQL other than this — `pg_cancel_backend(pid)` directly works too, but leaves the row at `FAIL` since nothing marked it `STOP` first.

`PLAN → TAKE` is a single `UPDATE ... SKIP LOCKED` that also counts current concurrency for the task's `group`/`remote` hash — via session-level advisory locks visible in `pg_locks` — against `max`. So `max` behaves less like a hard cap feeding one shared queue and more like a priority: a group with a higher `max` picks up its own next tasks sooner, independently of other groups. A negative `max` instead schedules a pause: on completion, the other `PLAN` rows of the same group get their `plan` pushed forward by `|max|` milliseconds.

`WORK → DONE/FAIL` is a single `UPDATE ... RETURNING` that, in the same round trip, decides whether to delete the row (`delete`, when both `output` and `error` are null), whether to insert the next `repeat` occurrence (computed from the original `plan` or from the actual finish time, depending on `drift`), whether the same worker process may pick up another task of the group without exiting (within `count`/`live`), and whether to reschedule the rest of the group (negative `max`).

`timeout` bounds how long `input` itself may run — locally via a timeout event in the `pg_task` worker's loop, remotely via `SET SESSION statement_timeout` sent ahead of `input`. `live`/`count` instead bound the executor *process*, not the task: how many tasks in a row, or how long, one `pg_task` worker lives before being recycled.

### Wake-up and crash recovery

Instead of `LISTEN`/`NOTIFY`, `pg_task` wakes idle workers with session-level advisory locks plus `pg_cancel_backend()`. Each `pg_work` process holds an advisory lock tagged with its group's hash for as long as it's alive; the `AFTER INSERT OR DELETE OR UPDATE OF plan` trigger (see below) looks up the holder of that lock in `pg_locks` and cancels it directly. `pg_work` installs its own `SIGINT` handler — instead of the default query-cancel one — that just sets the latch, so the wait-event loop returns immediately instead of waiting out the rest of `pg_task.sleep`. The `STOP`-on-a-remote-task trigger (see [Task state machine](#task-state-machine)) reuses this exact same wake-up: once woken, `pg_work` checks, at most once per `pg_task.sleep`, whether any of its currently active remote tasks now has `state = 'STOP'` and cancels that connection with `PQcancel()`.

A second, per-task advisory lock (tagged by the task's own `id`) is used to detect a crashed executor: every `pg_task.reset` interval, `pg_work` looks for rows still in `TAKE`/`WORK` whose `id`-tagged lock nobody currently holds, and resets them to `PLAN`. That's the crash-recovery mechanism.

When there's genuinely nothing to do, `pg_work` doesn't poll in a tight loop: it computes, in one query, the soonest moment something will actually need attention — the closer of the next `active`/`timeout` deadline among running tasks and the next `PLAN` task's `plan` — and sleeps exactly until then. `pg_task.sleep` is a floor on responsiveness for a busy queue, not a fixed polling interval.

### Self-provisioning and the helper triggers

On first connect, `pg_work` walks through a series of idempotent `SELECT EXISTS ...` checks against `pg_catalog` and issues the matching `CREATE`/`ALTER` only for what's missing: schema, the `state` enum, the table (with all columns, `current_setting('pg_task.…')`-backed defaults, `NOT NULL`/`CHECK` constraints), and indexes — including a functional index on the hash of `group`/`remote` that the concurrency accounting above relies on — plus, among others, three trigger functions of particular note:

- the **`user`-immutability trigger** (`BEFORE INSERT OR UPDATE OF "user"`) forces `NEW."user"` to `current_user` on insert unless the inserting role is a member of the claimed role, and rejects any later change — this is what makes the `user` column trustworthy for the Security considerations below.
- the **wake-up trigger** (`AFTER INSERT OR DELETE OR UPDATE OF plan`) is the mechanism described above; it does not use `NOTIFY`.
- the **`STOP` trigger** (`AFTER UPDATE OF "state"`) is what makes setting `state = 'STOP'` on a `WORK` row actually cancel it, as described in [Task state machine](#task-state-machine) above.

### Three ways to run `input`

There are three distinct execution paths, not two — which one applies is decided first by whether `remote` is set, and only then, for the non-remote case, by `pg_task.spi`:

- **local (no `remote`, default `spi = off`)** dispatches `input` through `exec_simple_query()` in the `pg_task` worker's own backend — the same function extracted from the matching version's `postgres.c` into `exec.c` at build time (see Build above) — i.e. the same multi-statement dispatcher PostgreSQL uses for a real client connection: full DDL, multiple `;`-separated statements, implicit transaction handling. Its result stream is captured by swapping in a custom `DestReceiver` that formats each row (honoring the task's `delimiter`/`quote`/`escape`/`null`/`string`) straight into `output`, turning command-completion tags (`UPDATE 3`, ...) into `output` lines too.
- **SPI (no `remote`, `spi = on`)** calls `SPI_execute()` directly in the same backend, instead of `exec_simple_query()` — faster, but strictly narrower (see the table below).
- **remote (`remote` is set)** bypasses both of the above entirely: `pg_task.spi` is not even read in this path. `pg_work` doesn't spawn a `pg_task` worker at all — it opens the connection itself, asynchronously and non-blocking (`PQconnectStartParams` + `PQsetnonblocking`), and adds its socket to its own wait-event set, so dozens of concurrent remote tasks cost no extra OS process beyond `pg_work` itself. Once connected it sends a preamble (`SET SESSION` for the relevant `pg_task.*` parameters and `statement_timeout`) followed by `input`, and `input` is dispatched by the remote server's own query processor over the wire, exactly as if a regular client had sent it — `pg_task`'s local `DestReceiver`/SPI code never runs. The streamed result is formatted into `output`/`error` the same way as in local mode, statement by statement. Once `input` finishes, `pg_work` sends `COMMIT` (closing whatever transaction `input` left open) and then `DISCARD ALL` unless `save` asks to keep the session for the next task of the group — before the connection is either reused or closed.

In every case the final state transition (`WORK → DONE/FAIL`) is written locally via SPI, regardless of which of the three paths actually ran `input`.

Each path has its own restrictions on what `input` can contain:

| | local | SPI | remote |
| --- | --- | --- | --- |
| Several `;`-separated statements | all results appended to `output` | all results appended to `output` (PostgreSQL 10+, split via `RawStmt.stmt_location`); only the *last* statement's result on pre-10 backends | all results appended to `output` |
| `COPY ... FROM STDIN` / `... TO STDOUT` / `COPY BOTH` | rejected outright (`COPY … is not supported`) | rejected outright (`SPI_ERROR_COPY`) | `FROM STDIN`/`BOTH` rejected (pg_task has no data to stream in); `TO STDOUT` **is** supported and streamed straight into `output` |
| `COPY ... TO/FROM` a server-side file or `PROGRAM` | allowed, if the role has the privilege | rejected (SPI rejects any `COPY`) | allowed, if the role has the privilege — runs on the remote server |
| Explicit `BEGIN`/`COMMIT`/`ROLLBACK` in `input` | allowed (a transaction left open at the end is closed automatically) | rejected (`SPI_ERROR_TRANSACTION`) | allowed (it's a real client session on the far side) |
| DDL | allowed | allowed (as an SPI utility statement) | allowed |

### Signals

`SIGHUP` — in `pg_conf`, `pg_work` and `pg_task` alike — reloads `postgresql.conf` and re-runs the relevant config/task check, so most GUCs and `pg_task.json` can be changed without a server restart. `SIGTERM` is handled the standard background-worker way; on the way out, each level releases its advisory locks, and `pg_work` additionally closes any open remote connections cleanly instead of dropping them.

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

GUCs, and most `task` columns, cascade through up to five levels — the config file, then per-database, per-role and per-session overrides, and finally the value on the row itself — the most specific one wins.

Limitations:
- if the executor crashes in the narrow window between `STOP` being set and the cancellation actually completing, the row can be left at `STOP` with `stop`/`error` never filled in — `work_reset` deliberately leaves `STOP` rows alone (unlike `TAKE`/`WORK`) so a cancelled task isn't accidentally resurrected back to `PLAN`;
- no built-in dashboard or UI — monitoring is `SELECT * FROM task` and, as needed, `pg_locks`/`pg_stat_activity`;
- tasks only run where the table can be written to — a read-only replica can't run a scheduler against it;
- no coordination across independent clusters — if the same `task` table or the same `remote` target is reachable from more than one place at once, consistency is on you; the advisory locks only protect against races inside one server instance;
- building requires network access to GitHub, or a pre-existing source tree (see Build above) — the extension pulls part of the matching version's real `postgres.c` at build time rather than just linking against an installed server;
- only the server branches/versions actually exercised by CI are supported — a brand-new major version may need a follow-up patch before the code extracted from its `postgres.c` matches again;
- with the default `spi = off`, a task has exactly the privileges of a normal client session for its role, including `COPY ... TO/FROM PROGRAM` if the role can do that — this follows directly from running `input` as a real client would, not from a bug; if task authors aren't fully trusted, use `spi = on` or restrict the role's privileges (see Security considerations below).

## Security considerations

`pg_task` executes the raw SQL text stored in `task.input` as the role recorded in `task.user`. That column defaults to `current_user` at insert time, is force-overwritten to `current_user` by a `BEFORE INSERT` trigger regardless of what value the client supplies, and is immutable afterwards (a `BEFORE UPDATE OF "user"` trigger rejects any change). Since `task.user` is trustworthy, execution switches to it (`SET ROLE`) only for the duration of running `input`, then reverts to the worker's own connecting role (configured via `pg_task.user`, default: `postgres`) before any further housekeeping (recording `state`/`output`/`error`, scheduling repeats, etc.).

This means **a task only ever runs with the privileges of whoever actually inserted it**, provided the connecting role (`pg_task.user`) is not itself granted broad membership in every role — see below.

Things to keep in mind:

- **`pg_task.user` no longer needs to be a superuser, or even privileged**, to let different authors run tasks with their own rights — `SET ROLE` only requires plain membership (or, on PostgreSQL 16+, membership `WITH SET TRUE`) rather than superuser. Grant `pg_task.user` membership only in the specific roles it should be allowed to execute tasks as: `GRANT author_role TO pg_task_user WITH INHERIT FALSE, SET TRUE;`. A role with `INSERT` on `task` but no such grant to `pg_task.user` cannot get its tasks executed with anyone else's privileges — the `SET ROLE` inside the worker simply fails and the task ends up `FAIL`.
- **If you do keep `pg_task.user` privileged** (e.g. `postgres`), it is still capable of running as any role on the server — restrict who can `INSERT` into `task` accordingly (`REVOKE INSERT ON task FROM PUBLIC` and grant it selectively, or only allow inserts through a `SECURITY DEFINER` wrapper function). The `user` column protects against forged *identity*, not against a task simply running with its author's genuinely-granted privileges.
- **`pg_task.spi = off` (the default) executes `input` as a normal top-level statement**, so anything the effective role (`task.user`) is allowed to do is possible — including `COPY ... TO`/`FROM` a server-side file or program if that role has the required privilege. `pg_task.spi = on` runs `input` through SPI instead, where `COPY` in any form is rejected outright (`SPI_ERROR_COPY`), which removes that specific class of vector at the cost of SPI's other limitations (no explicit transaction control in `input`, only the last statement's result on pre-PostgreSQL 10 backends, etc.).
- **The `remote` column's password requirement now reflects the actual task author.** When a task sets `remote`, `pg_task` refuses to connect without a password unless `task.user` is a superuser (`work.c`'s `work_remote`). Once connected, execution on the remote server is governed entirely by the credentials in the `remote` connection string — `task.user`/`SET ROLE` have no effect there, since the remote server has its own, independent role catalog. Don't put passwordless/trust-authenticated connection strings within reach of task authors you don't fully trust.
- Review which extensions/functions any role granted to `pg_task.user` can execute (`dblink`, `adminpack`, large-object functions, `pg_read_file`, etc.) and revoke what isn't needed.
- Consider auditing (e.g. `pgaudit`) or alerting on `task.input` containing sensitive constructs (`COPY ... PROGRAM`, `CREATE FUNCTION ... LANGUAGE c`, `ALTER SYSTEM`, `dblink`, ...) if task authorship is not fully trusted.
