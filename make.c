#include "include.h"

#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_trigger.h>
#include <libpq/libpq-be.h>
#include <mb/pg_wchar.h>
#include <parser/parse_type.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/proc.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>

#if PG_VERSION_NUM >= 100000
#include <utils/regproc.h>
#else
#include <access/hash.h>
#endif

#if PG_VERSION_NUM >= 130000
#include <postmaster/interrupt.h>
#else
#include <catalog/pg_type.h>
#include <miscadmin.h>
#endif

#if PG_VERSION_NUM < 140000
#include <utils/timestamp.h>
#endif

#if PG_VERSION_NUM < 150000
#include <utils/rel.h>
#endif

// to act as a role, as a task runs as its author, or to see its tasks: from 16 on, MEMBER is any membership, also one WITH SET FALSE, which doesn't let SET ROLE to it, as pg_task.user is checked with in work_owner(), while before it every membership did
#if PG_VERSION_NUM >= 160000
#define MAKE_ACT "SET"
#else
#define MAKE_ACT "MEMBER"
#endif

volatile sig_atomic_t make_lock_timeout = false; // the DDL below is running, whose lock timeout pg_work's own SIGINT handler, which lock timeouts signal through, is to let through, see work_idle()

static void make_ddl(const char *src, int res) {
    ResourceOwner oldowner = CurrentResourceOwner;
    MemoryContext oldcontext = CurrentMemoryContext;
    bool ok = false;
    SPI_connect_my(src, InvalidOid);
    SPI_execute_with_args_my(SQL(SET LOCAL "lock_timeout" = 2000), 0, NULL, NULL, NULL, SPI_OK_UTILITY); // not to wait for long on a table busy with its tasks, but only for this transaction, whose end, commit or abort, gives pg_work back its own, the server's, its database's or its role's
    make_lock_timeout = true;
    for (int attempt = 1; !ok && attempt <= 5; attempt++) {
        BeginInternalSubTransaction(NULL);
        MemoryContextSwitchTo(oldcontext);
        PG_TRY();
            SPI_execute_with_args_my(src, 0, NULL, NULL, NULL, res);
            ReleaseCurrentSubTransaction();
            MemoryContextSwitchTo(oldcontext);
            CurrentResourceOwner = oldowner;
            ok = true;
        PG_CATCH();
            {
                ErrorData *edata;
                MemoryContextSwitchTo(oldcontext);
                edata = CopyErrorData();
                FlushErrorState();
                RollbackAndReleaseCurrentSubTransaction();
                MemoryContextSwitchTo(oldcontext);
                CurrentResourceOwner = oldowner;
#if PG_VERSION_NUM < 100000
                SPI_restore_connection();
#endif
                if (edata->sqlerrcode != ERRCODE_LOCK_NOT_AVAILABLE || attempt == 5) { make_lock_timeout = false; ReThrowError(edata); }
                elog(DEBUG1, "lock not available, attempt = %i, src = %s", attempt, src);
                FreeErrorData(edata);
                pg_usleep(200000L);
            }
        PG_END_TRY();
    }
    make_lock_timeout = false;
    SPI_finish_my();
}

static Oid make_oid(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls) {
    Oid oid;
    SPI_connect_my(src, InvalidOid);
    SPI_execute_with_args_my(src, nargs, argtypes, values, nulls, SPI_OK_SELECT);
    if (SPI_processed != 1) ereport(ERROR, (errmsg("SPI_processed %lu != 1", (long)SPI_processed)));
    oid = DatumGetObjectId(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "oid", false, OIDOID));
    SPI_finish_my();
    return oid;
}

static bool make_test(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls) {
    bool test;
    SPI_connect_my(src, InvalidOid);
    SPI_execute_with_args_my(src, nargs, argtypes, values, nulls, SPI_OK_SELECT);
    if (SPI_processed != 1) ereport(ERROR, (errmsg("SPI_processed %lu != 1", (long)SPI_processed)));
    test = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "test", false, BOOLOID));
    SPI_finish_my();
    return test;
}

// an object of pg_task's that someone else made before pg_work did, in public, say, where anyone could create one before 15, is theirs to change still, the body of a function, which CREATE OR REPLACE leaves theirs, the table, its policy or its enum of states, and so to run what they like as pg_task.user or as the authors of tasks: rather than take it in, refuse unless it's owned by pg_task.user or a superuser
static void make_owner(const char *what, const char *name, const char *src, int nargs, Oid *argtypes, Datum *values) {
    SPI_connect_my(src, InvalidOid);
    SPI_execute_with_args_my(src, nargs, argtypes, values, NULL, SPI_OK_SELECT);
    if (SPI_processed) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("%s %s exists and is owned by %s, not by pg_task.user or a superuser", what, name, SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1)), errhint("Have it owned by pg_task.user (ALTER ... OWNER TO), if it's to be trusted with the tasks of others, or else drop it.")));
    SPI_finish_my();
}

void make_schema(const Work *w) {
    Datum values[] = {CStringGetTextDatum(w->shared->schema)};
    static Oid argtypes[] = {TEXTOID};
    StringInfoData src;
    set_ps_display_my("schema");
    initStringInfoMy(&src);
    // its owner may drop the objects of others in it, the task table of pg_task say, for one of its own, whose triggers then run as pg_task.user: pg_work refuses it too, unless owned by pg_task.user or a superuser, as the objects of its own; that of public from 15 on, pg_database_owner, being the owner of the database
    appendStringInfo(&src, SQL(
        SELECT r.rolname::pg_catalog.text AS "owner" FROM pg_catalog.pg_namespace AS n JOIN pg_catalog.pg_roles AS r ON r.oid OPERATOR(pg_catalog.=) CASE WHEN n.nspowner OPERATOR(pg_catalog.=) (SELECT "oid" FROM pg_catalog.pg_roles WHERE rolname OPERATOR(pg_catalog.=) 'pg_database_owner') THEN (SELECT datdba FROM pg_catalog.pg_database WHERE datname OPERATOR(pg_catalog.=) pg_catalog.current_database()) ELSE n.nspowner END WHERE nspname OPERATOR(pg_catalog.=) $1 AND r.rolname OPERATOR(pg_catalog.<>) current_user AND NOT r.rolsuper
    ));
    make_owner("schema", w->schema, src.data, countof(argtypes), argtypes, values);
    resetStringInfo(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_namespace WHERE nspname OPERATOR(pg_catalog.=) $1) AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE SCHEMA %s;
        ), w->schema);
        make_ddl(src.data, SPI_OK_UTILITY);
    }
    pfree(src.data);
    pfree((void *)values[0]);
    set_ps_display_my("idle");
}

// the value, which has the names of the schema in it, those of its type, as a parameter, not in the text of the query, as anything in the name of the schema would end a quote, $$ say, which pg_task.schema may come with from the settings of the database, which its owner may change
static void make_default(const Work *w, const char *name, const char *value) {
    Datum values[] = {CStringGetTextDatum(value)};
    static Oid argtypes[] = {TEXTOID};
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT (SELECT pg_catalog.pg_get_expr(adbin, adrelid) FROM pg_catalog.pg_attribute JOIN pg_catalog.pg_attrdef ON attrelid OPERATOR(pg_catalog.=) adrelid WHERE attnum OPERATOR(pg_catalog.=) adnum AND attrelid OPERATOR(pg_catalog.=) %1$i AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) '%2$s') IS NOT DISTINCT FROM $1 AS "test"
    ), w->shared->oid, name);
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            ALTER TABLE %1$s ALTER COLUMN "%2$s" SET DEFAULT %3$s;
            UPDATE %1$s SET "%2$s" = DEFAULT WHERE "%2$s" IS NULL;
        ), w->schema_table, name, value);
        make_ddl(src.data, SPI_OK_UPDATE);
    }
    pfree(src.data);
    pfree((void *)values[0]);
}

// whether the column has a check of ours, not just any: one of the user's own on the column too must neither break the lookup nor pass for ours
static void make_constraint(const Work *w, const char *name, const char *value, const char *type) {
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_constraint JOIN pg_catalog.pg_attribute ON attrelid OPERATOR(pg_catalog.=) conrelid WHERE attnum OPERATOR(pg_catalog.=) conkey[1] AND attrelid OPERATOR(pg_catalog.=) %1$i AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) '%2$s' AND conbin IS NOT NULL AND pg_catalog.pg_get_expr(conbin, conrelid) OPERATOR(pg_catalog.=) $$(%2$s %3$s)$$) AS "test"
    ), w->shared->oid, name, value);
    if (!make_test(src.data, 0, NULL, NULL, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            ALTER TABLE %1$s ADD CHECK ("%2$s" %3$s%4$s);
        ), w->schema_table, name, value, type ? type : "");
        make_ddl(src.data, SPI_OK_UTILITY);
    }
    pfree(src.data);
}

// not only the body: security definer and search_path may change without it, and a function created otherwise by an earlier version would be kept
static void make_function(const Work *w, const char *name, const char *source, bool security_definer) {
    Datum values[] = {CStringGetTextDatum(name), CStringGetTextDatum(w->shared->schema), CStringGetTextDatum(source), BoolGetDatum(security_definer)};
    static Oid argtypes[] = {TEXTOID, TEXTOID, TEXTOID, BOOLOID};
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT r.rolname::pg_catalog.text AS "owner" FROM pg_catalog.pg_proc JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) pronamespace JOIN pg_catalog.pg_roles r ON r.oid OPERATOR(pg_catalog.=) proowner WHERE proname OPERATOR(pg_catalog.=) $1 AND nspname OPERATOR(pg_catalog.=) $2 AND pronargs OPERATOR(pg_catalog.=) 0 AND r.rolname OPERATOR(pg_catalog.<>) current_user AND NOT r.rolsuper
    ));
    {
        const char *quote = quote_identifier(name);
        StringInfoData function;
        initStringInfoMy(&function);
        appendStringInfo(&function, "%s.%s()", w->schema, quote);
        make_owner("function", function.data, src.data, 2, argtypes, values);
        pfree(function.data);
        if (quote != name) pfree((void *)quote);
    }
    resetStringInfo(&src);
    // the trigger function, with no arguments, not one of the same name with some, which someone may have made too
    appendStringInfo(&src, SQL(
        SELECT COALESCE((SELECT prosrc OPERATOR(pg_catalog.=) $3 AND prosecdef OPERATOR(pg_catalog.=) $4 AND proconfig OPERATOR(pg_catalog.=) ARRAY['search_path=pg_catalog, pg_temp']::pg_catalog.text[] FROM pg_catalog.pg_proc JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) pronamespace WHERE proname OPERATOR(pg_catalog.=) $1 AND nspname OPERATOR(pg_catalog.=) $2 AND pronargs OPERATOR(pg_catalog.=) 0), false) AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        const char *quote = quote_identifier(name);
        const char *quote_source = quote_literal_cstr(source); // the body has the names of the schema in it, which would end a dollar quote, see make_default()
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE OR REPLACE FUNCTION %1$s.%2$s() RETURNS TRIGGER SET search_path = pg_catalog, pg_temp %4$s AS %3$s LANGUAGE plpgsql;
        ), w->schema, quote, quote_source, security_definer ? "SECURITY DEFINER" : "SECURITY INVOKER");
        make_ddl(src.data, SPI_OK_UTILITY);
        if (quote != name) pfree((void *)quote);
        pfree((void *)quote_source);
    }
    pfree(src.data);
    pfree((void *)values[0]);
    pfree((void *)values[1]);
    pfree((void *)values[2]);
}

// type is made of the TRIGGER_TYPE_* bits, and column is the one of UPDATE OF, if any: both are checked against the trigger by that name, which is re-created when it fires otherwise, as one made by an earlier version may
static void make_trigger(const Work *w, const char *name, int16 type, const char *column) {
    Datum values[] = {CStringGetTextDatum(name), ObjectIdGetDatum(w->shared->oid), Int16GetDatum(type), column ? CStringGetTextDatum(column) : (Datum)0};
    char nulls[] = {' ', ' ', ' ', column ? ' ' : 'n'}; // column: those of UPDATE OF, in order, separated by commas
    static Oid argtypes[] = {TEXTOID, OIDOID, INT2OID, TEXTOID};
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT COALESCE((SELECT tgtype OPERATOR(pg_catalog.=) $3 AND tgattr::pg_catalog.text OPERATOR(pg_catalog.=) pg_catalog.array_to_string(ARRAY(SELECT attnum FROM pg_catalog.unnest(pg_catalog.string_to_array($4, ',')) WITH ORDINALITY AS c ("name", "i") JOIN pg_catalog.pg_attribute ON attname OPERATOR(pg_catalog.=) c."name" WHERE attrelid OPERATOR(pg_catalog.=) $2 AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped ORDER BY c."i"), ' ') FROM pg_catalog.pg_trigger WHERE tgname OPERATOR(pg_catalog.=) $1 AND tgrelid OPERATOR(pg_catalog.=) $2), false) AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, nulls)) {
        const char *quote = quote_identifier(name);
        const char *sep = " ";
        StringInfoData when;
        initStringInfoMy(&when);
        appendStringInfoString(&when, TRIGGER_FOR_BEFORE(type) ? "BEFORE" : "AFTER");
        if (TRIGGER_FOR_INSERT(type)) { appendStringInfo(&when, "%sINSERT", sep); sep = " OR "; }
        if (TRIGGER_FOR_DELETE(type)) { appendStringInfo(&when, "%sDELETE", sep); sep = " OR "; }
        if (TRIGGER_FOR_UPDATE(type)) appendStringInfo(&when, "%sUPDATE", sep);
        if (column) for (const char *c = column, *of = " OF "; *c; of = ", ") { // the columns, in the order tgattr keeps them in, see above
            const char *end = strchr(c, ',');
            int len = end ? end - c : strlen(c);
            appendStringInfo(&when, "%s\"%.*s\"", of, len, c);
            c += len + (end ? 1 : 0);
        }
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            DROP TRIGGER IF EXISTS %1$s ON %3$s;
            CREATE TRIGGER %1$s %2$s ON %3$s FOR EACH %4$s EXECUTE PROCEDURE %5$s.%1$s();
        ), quote, when.data, w->schema_table, TRIGGER_FOR_ROW(type) ? "ROW" : "STATEMENT", w->schema);
        make_ddl(src.data, SPI_OK_UTILITY);
        if (quote != name) pfree((void *)quote);
        pfree(when.data);
    }
    pfree(src.data);
    pfree((void *)values[0]);
    if (column) pfree((void *)values[3]);
}

// trigger and function names are <table>_<suffix>, which PostgreSQL would silently truncate to NAMEDATALEN - 1, making different suffixes collide and the existence checks never find what they created: when that doesn't fit, clip the table part and keep it unique with the table's hash
static void make_name(const Work *w, StringInfo name, const char *suffix) {
    int len = strlen(w->shared->table);
    if (len + 1 + strlen(suffix) <= NAMEDATALEN - 1) appendStringInfo(name, "%s_%s", w->shared->table, suffix);
    else appendStringInfo(name, "%.*s_%08x_%s", pg_mbcliplen(w->shared->table, len, NAMEDATALEN - 1 - (int)strlen("_12345678_") - (int)strlen(suffix)), w->shared->table, (uint32)w->shared->hash, suffix);
}

static void make_wake_up(const Work *w) {
    StringInfoData name;
    StringInfoData source;
    initStringInfoMy(&name);
    make_name(w, &name, "wake_up");
    initStringInfoMy(&source);
    appendStringInfo(&source, SQL(
        BEGIN
            BEGIN
                PERFORM pg_catalog.pg_cancel_backend(pid) FROM "pg_catalog"."pg_locks" WHERE "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 3 AND "database" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_database" WHERE "datname" OPERATOR(pg_catalog.=) current_catalog) AND "objid" OPERATOR(pg_catalog.=) %1$i;
            EXCEPTION WHEN insufficient_privilege THEN NULL;
            END;
            RETURN %2$s;
        END;
    ), w->shared->hash,
#ifdef GP_VERSION_NUM
"NEW"
#else
"NULL"
#endif
    );
    make_function(w, name.data, source.data, true);
    make_trigger(w, name.data, TRIGGER_TYPE_AFTER | TRIGGER_TYPE_INSERT | TRIGGER_TYPE_DELETE | TRIGGER_TYPE_UPDATE
#ifdef GP_VERSION_NUM
        | TRIGGER_TYPE_ROW
#endif
    , "plan");
    pfree(name.data);
    pfree(source.data);
}

// only wakes pg_work up, which then cancels the task (work_stop), local or remote alike: a local task runs as its author, whose backend pg_task.user may not signal from SQL, and a single canceller never signals a task twice
static void make_stop(const Work *w) {
    StringInfoData name;
    StringInfoData source;
    initStringInfoMy(&name);
    make_name(w, &name, "stop");
    initStringInfoMy(&source);
    appendStringInfo(&source, SQL(
        BEGIN
            IF OLD."state" OPERATOR(pg_catalog.=) 'WORK' AND NEW."state" OPERATOR(pg_catalog.=) 'STOP' THEN
                BEGIN
                    PERFORM pg_catalog.pg_cancel_backend(pid) FROM "pg_catalog"."pg_locks" WHERE "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 3 AND "database" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_database" WHERE "datname" OPERATOR(pg_catalog.=) current_catalog) AND "objid" OPERATOR(pg_catalog.=) %1$i;
                EXCEPTION WHEN insufficient_privilege THEN NULL;
                END;
            END IF;
            RETURN NEW;
        END;
    ), w->shared->hash);
    make_function(w, name.data, source.data, true);
    make_trigger(w, name.data, TRIGGER_TYPE_AFTER | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_ROW, "state");
    pfree(name.data);
    pfree(source.data);
}

// the table owner (pg_task.user) keeps the user it inserts, for the repeats it copies: it isn't bound by this trigger anyway, being able to change its own table's triggers, and pg_work checks it may act as the user before running a local task
static void make_user_immutable(const Work *w) {
    StringInfoData name;
    StringInfoData source;
    initStringInfoMy(&name);
    make_name(w, &name, "user");
    initStringInfoMy(&source);
    appendStringInfo(&source, SQL(
        BEGIN
            IF TG_OP OPERATOR(pg_catalog.=) 'INSERT' THEN
                BEGIN
                    IF NOT pg_catalog.pg_has_role(current_user, NEW."user", '%1$s') AND NOT pg_catalog.pg_has_role(current_user, (SELECT "relowner" FROM "pg_catalog"."pg_class" WHERE "oid" OPERATOR(pg_catalog.=) TG_RELID), '%1$s') THEN NEW."user" := current_user; END IF;
                EXCEPTION WHEN undefined_object THEN NEW."user" := current_user;
                END;
            ELSIF NEW."user" IS DISTINCT FROM OLD."user" THEN RAISE EXCEPTION 'user column is immutable';
            END IF;
            RETURN NEW;
        END;
    ), MAKE_ACT);
    make_function(w, name.data, source.data, false);
    make_trigger(w, name.data, TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSERT | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_ROW, "user");
    pfree(name.data);
    pfree(source.data);
}

// the table owner (pg_task.user), whose bookkeeping drives a task through its states, and its members may make every transition of the state machine; anyone else, a task author, only the one to STOP, of a task still queued or running
static void make_state_machine(const Work *w) {
    StringInfoData name;
    StringInfoData source;
    initStringInfoMy(&name);
    make_name(w, &name, "state");
    initStringInfoMy(&source);
    appendStringInfo(&source, SQL(
        BEGIN
            IF NEW."state" OPERATOR(pg_catalog.<>) OLD."state" AND NEW."state" OPERATOR(pg_catalog.<>) ALL (CASE WHEN pg_catalog.pg_has_role(current_user, (SELECT "relowner" FROM "pg_catalog"."pg_class" WHERE "oid" OPERATOR(pg_catalog.=) TG_RELID), '%2$s') THEN CASE OLD."state"
                WHEN 'PLAN'::%1$s THEN ARRAY['TAKE', 'GONE', 'STOP']::%1$s[]
                WHEN 'TAKE'::%1$s THEN ARRAY['WORK', 'PLAN', 'DONE', 'FAIL']::%1$s[]
                WHEN 'WORK'::%1$s THEN ARRAY['DONE', 'FAIL', 'PLAN', 'STOP']::%1$s[]
                ELSE ARRAY[]::%1$s[]
            END WHEN OLD."state" OPERATOR(pg_catalog.=) ANY(ARRAY['PLAN', 'WORK']::%1$s[]) THEN ARRAY['STOP']::%1$s[] ELSE ARRAY[]::%1$s[] END) THEN RAISE EXCEPTION 'invalid state transition';
            END IF;
            RETURN NEW;
        END;
    ), w->schema_type, MAKE_ACT);
    make_function(w, name.data, source.data, false);
    make_trigger(w, name.data, TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_ROW, "state");
    pfree(name.data);
    pfree(source.data);
}

// pg_work adds up plan, active, live, repeat and timeout, and on values out of range that fails, taking down every task it runs: have the author's own insert or update fail on them instead
// and on an interval of a part less than 0, of parts of both signs ('-1 mon 31 days'), which, more than 0 as it is, may take the plan nowhere, or back, from some days, a repeat say, see task_insert(): only an interval inserted or changed, compared as text, as '1 mon' is equal to '30 days', not one of a row from before this check, which an update of another column, of the state by pg_work say, would fail on otherwise, nor the copy of a repeat with the intervals of the task before it, which pg_work inserts, its bookkeeping failing on them otherwise; the task before it looked up only then, in a statement of its own, as the author inserting a task may have no right to read the table, which a query is checked for whether it gets to read it or not
static void make_valid(const Work *w) {
    static const char *columns[] = {"active", "live", "repeat", "timeout"};
    StringInfoData name;
    StringInfoData source;
    initStringInfoMy(&name);
    make_name(w, &name, "valid");
    initStringInfoMy(&source);
    appendStringInfoString(&source, SQL(
        BEGIN
            IF TG_OP OPERATOR(pg_catalog.=) 'UPDATE' THEN
                IF NEW."plan" IS NOT DISTINCT FROM OLD."plan" AND NEW."active"::pg_catalog.text IS NOT DISTINCT FROM OLD."active"::pg_catalog.text AND NEW."live"::pg_catalog.text IS NOT DISTINCT FROM OLD."live"::pg_catalog.text AND NEW."repeat"::pg_catalog.text IS NOT DISTINCT FROM OLD."repeat"::pg_catalog.text AND NEW."timeout"::pg_catalog.text IS NOT DISTINCT FROM OLD."timeout"::pg_catalog.text THEN RETURN NEW;
                END IF;
            END IF;
            PERFORM NEW."plan" OPERATOR(pg_catalog.+) (NEW."active" OPERATOR(pg_catalog.+) NEW."live" OPERATOR(pg_catalog.+) NEW."repeat" OPERATOR(pg_catalog.+) NEW."timeout"), pg_catalog.statement_timestamp() OPERATOR(pg_catalog.+) (NEW."active" OPERATOR(pg_catalog.+) NEW."live" OPERATOR(pg_catalog.+) NEW."repeat" OPERATOR(pg_catalog.+) NEW."timeout");
    ));
    for (int i = 0; i < (int)countof(columns); i++) appendStringInfo(&source, SQL(
            IF EXTRACT(year FROM NEW."%1$s") OPERATOR(pg_catalog.<) 0 OR EXTRACT(month FROM NEW."%1$s") OPERATOR(pg_catalog.<) 0 OR EXTRACT(day FROM NEW."%1$s") OPERATOR(pg_catalog.<) 0 OR EXTRACT(hour FROM NEW."%1$s") OPERATOR(pg_catalog.<) 0 OR EXTRACT(minute FROM NEW."%1$s") OPERATOR(pg_catalog.<) 0 OR EXTRACT(second FROM NEW."%1$s") OPERATOR(pg_catalog.<) 0 THEN
                IF TG_OP OPERATOR(pg_catalog.=) 'INSERT' THEN
                    IF NEW."parent" IS NULL OR NOT pg_catalog.has_table_privilege(TG_RELID, 'SELECT') THEN RAISE EXCEPTION '%1$s column has a part less than 0';
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM %2$s AS p WHERE p."id" OPERATOR(pg_catalog.=) NEW."parent" AND p."%1$s"::pg_catalog.text OPERATOR(pg_catalog.=) NEW."%1$s"::pg_catalog.text) THEN RAISE EXCEPTION '%1$s column has a part less than 0';
                    END IF;
                ELSIF NEW."%1$s"::pg_catalog.text IS DISTINCT FROM OLD."%1$s"::pg_catalog.text THEN RAISE EXCEPTION '%1$s column has a part less than 0';
                END IF;
            END IF;
    ), columns[i], w->schema_table);
    appendStringInfoString(&source, SQL(
            RETURN NEW;
        END;
    ));
    make_function(w, name.data, source.data, false);
    make_trigger(w, name.data, TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSERT | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_ROW, "plan,active,live,repeat,timeout"); // not for an update of its state by pg_work, say, a call of the function for each row: the only columns it checks
    pfree(name.data);
    pfree(source.data);
}

static void make_immutable(const Work *w, const char *column) {
    StringInfoData name;
    StringInfoData source;
    initStringInfoMy(&name);
    make_name(w, &name, column);
    initStringInfoMy(&source);
    appendStringInfo(&source, SQL(
        BEGIN
            IF NEW."%1$s" IS DISTINCT FROM OLD."%1$s" THEN RAISE EXCEPTION '%1$s column is immutable';
            END IF;
            RETURN NEW;
        END;
    ), column);
    make_function(w, name.data, source.data, false);
    make_trigger(w, name.data, TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_ROW, column);
    pfree(name.data);
    pfree(source.data);
}

static void make_conditional_immutable(const Work *w, const char *column) {
    StringInfoData name;
    StringInfoData source;
    initStringInfoMy(&name);
    make_name(w, &name, column);
    initStringInfoMy(&source);
    appendStringInfo(&source, SQL(
        BEGIN
            IF OLD."state" OPERATOR(pg_catalog.<>) 'PLAN'::%2$s AND NEW."%1$s" IS DISTINCT FROM OLD."%1$s" THEN RAISE EXCEPTION '%1$s column is immutable once task left PLAN state';
            END IF;
            RETURN NEW;
        END;
    ), column, w->schema_type);
    make_function(w, name.data, source.data, false);
    make_trigger(w, name.data, TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_ROW, column);
    pfree(name.data);
    pfree(source.data);
}

#if PG_VERSION_NUM >= 90500
// a role only sees and changes the tasks it may act as, the same way the user column's trigger lets it insert them, a member of the table owner (even without inheriting its rights, which would exempt it from the policy altogether) included: otherwise anyone with UPDATE on the table could rewrite the input of someone else's queued task and have it run as that someone; the table owner (pg_task.user) isn't bound by it, so pg_work and the task's bookkeeping still see every task, and a dropped role's tasks are just hidden rather than erroring out the whole query as pg_has_role by name would; pg_get_expr deparses the policy differently across versions and search paths, so the policy's comment keeps the expression it was made with, and a policy whose comment differs is brought up to date
static void make_policy(const Work *w) {
    Datum values[3];
    static Oid argtypes[] = {TEXTOID, OIDOID, TEXTOID};
    StringInfoData expr;
    StringInfoData name;
    StringInfoData src;
    initStringInfoMy(&expr);
    {
        const char *quote_table = quote_literal_cstr(w->schema_table);
        appendStringInfo(&expr, "\"user\" OPERATOR(pg_catalog.=) CURRENT_USER OR pg_catalog.pg_has_role((SELECT \"oid\" FROM \"pg_catalog\".\"pg_roles\" WHERE \"rolname\" OPERATOR(pg_catalog.=) \"user\"), '%2$s') OR pg_catalog.pg_has_role((SELECT \"relowner\" FROM \"pg_catalog\".\"pg_class\" WHERE \"oid\" OPERATOR(pg_catalog.=) %1$s::pg_catalog.regclass), '%2$s')", quote_table, MAKE_ACT);
        pfree((void *)quote_table);
    }
    initStringInfoMy(&name);
    make_name(w, &name, "user");
    values[0] = CStringGetTextDatum(name.data);
    values[1] = ObjectIdGetDatum(w->shared->oid);
    values[2] = CStringGetTextDatum(expr.data);
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_policy WHERE polname OPERATOR(pg_catalog.=) $1 AND polrelid OPERATOR(pg_catalog.=) $2) AS "test"
    ));
    if (!make_test(src.data, 2, argtypes, values, NULL)) {
        const char *quote = quote_identifier(name.data);
        const char *quote_expr = quote_literal_cstr(expr.data);
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE POLICY %1$s ON %2$s USING (%3$s) WITH CHECK (%3$s);
            COMMENT ON POLICY %1$s ON %2$s IS %4$s;
        ), quote, w->schema_table, expr.data, quote_expr);
        make_ddl(src.data, SPI_OK_UTILITY);
        if (quote != name.data) pfree((void *)quote);
        pfree((void *)quote_expr);
    } else {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            SELECT (SELECT "description" FROM pg_catalog.pg_policy JOIN pg_catalog.pg_description ON objoid OPERATOR(pg_catalog.=) pg_catalog.pg_policy.oid WHERE classoid OPERATOR(pg_catalog.=) 'pg_catalog.pg_policy'::pg_catalog.regclass AND objsubid OPERATOR(pg_catalog.=) 0 AND polname OPERATOR(pg_catalog.=) $1 AND polrelid OPERATOR(pg_catalog.=) $2) IS NOT DISTINCT FROM $3 AS "test"
        ));
        if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
            const char *quote = quote_identifier(name.data);
            const char *quote_expr = quote_literal_cstr(expr.data);
            resetStringInfo(&src);
            appendStringInfo(&src, SQL(
                ALTER POLICY %1$s ON %2$s USING (%3$s) WITH CHECK (%3$s);
                COMMENT ON POLICY %1$s ON %2$s IS %4$s;
            ), quote, w->schema_table, expr.data, quote_expr);
            make_ddl(src.data, SPI_OK_UTILITY);
            if (quote != name.data) pfree((void *)quote);
            pfree((void *)quote_expr);
        }
    }
    resetStringInfo(&src);
    appendStringInfo(&src, SQL(
        SELECT relrowsecurity AS "test" FROM pg_catalog.pg_class WHERE oid OPERATOR(pg_catalog.=) %1$i
    ), w->shared->oid);
    if (!make_test(src.data, 0, NULL, NULL, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            ALTER TABLE %1$s ENABLE ROW LEVEL SECURITY;
        ), w->schema_table);
        make_ddl(src.data, SPI_OK_UTILITY);
    }
    pfree(expr.data);
    pfree(name.data);
    pfree(src.data);
    pfree((void *)values[0]);
    pfree((void *)values[2]);
}
#endif

// the hash column of earlier versions, which the index on the hash replaced: an int4 one, generated from 12 on and filled by the <table>_hash_generate trigger before; drop it along with that trigger, but leave a hash column of the user's own alone
static void make_legacy_hash(const Work *w) {
    Datum values[1];
    static Oid argtypes[] = {TEXTOID};
    StringInfoData name;
    StringInfoData src;
    initStringInfoMy(&name);
    appendStringInfo(&name, "%s_hash_generate", w->shared->table);
    if (name.len >= NAMEDATALEN) name.data[name.len = pg_mbcliplen(name.data, name.len, NAMEDATALEN - 1)] = '\0'; // as PostgreSQL truncated it on creating the trigger
    values[0] = CStringGetTextDatum(name.data);
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_attribute WHERE attrelid OPERATOR(pg_catalog.=) %1$i AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) 'hash' AND atttypid OPERATOR(pg_catalog.=) 'pg_catalog.int4'::pg_catalog.regtype AND (%2$s OR EXISTS (SELECT * FROM pg_catalog.pg_trigger WHERE tgrelid OPERATOR(pg_catalog.=) %1$i AND tgname OPERATOR(pg_catalog.=) $1))) AS "test"
    ), w->shared->oid,
#if PG_VERSION_NUM >= 120000
        "attgenerated OPERATOR(pg_catalog.=) 's'"
#else
        "false"
#endif
    );
    if (make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        const char *quote = quote_identifier(name.data);
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            DROP TRIGGER IF EXISTS %1$s ON %2$s;
            DROP FUNCTION IF EXISTS %3$s.%1$s();
            ALTER TABLE %2$s DROP COLUMN "hash";
        ), quote, w->schema_table, w->schema);
        make_ddl(src.data, SPI_OK_UTILITY);
        if (quote != name.data) pfree((void *)quote);
    }
    pfree(name.data);
    pfree(src.data);
    pfree((void *)values[0]);
}

static void make_column(const Work *w, const char *name, const char *schema_type) {
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT %3$s EXISTS (SELECT * FROM pg_catalog.pg_attribute WHERE attrelid OPERATOR(pg_catalog.=) %1$i AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) '%2$s') AS "test"
    ), w->shared->oid, name, schema_type ? "NOT" : "");
    if (make_test(src.data, 0, NULL, NULL, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            ALTER TABLE %1$s %2$s COLUMN "%3$s" %4$s;
        ), w->schema_table, schema_type ? "ADD" : "DROP", name, schema_type ? schema_type : "");
        make_ddl(src.data, SPI_OK_UTILITY);
    }
    pfree(src.data);
}

static void make_not_null(const Work *w, const char *name, bool not_null) {
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT (SELECT attnotnull FROM pg_catalog.pg_attribute WHERE attrelid OPERATOR(pg_catalog.=) %1$i AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) '%2$s') IS NOT DISTINCT FROM %3$s AS "test"
    ), w->shared->oid, name, not_null ? "true" : "false");
    if (!make_test(src.data, 0, NULL, NULL, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            ALTER TABLE %1$s ALTER COLUMN "%2$s" %3$s NOT NULL;
        ), w->schema_table, name, not_null ? "SET" : "DROP");
        make_ddl(src.data, SPI_OK_UTILITY);
    }
    pfree(src.data);
}

static void make_index(const Work *w, const char *name) {
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_index JOIN pg_catalog.pg_attribute ON attrelid OPERATOR(pg_catalog.=) indrelid WHERE attnum OPERATOR(pg_catalog.=) indkey[0] AND attrelid OPERATOR(pg_catalog.=) %1$i AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) '%2$s') AS "test"
    ), w->shared->oid, name);
    if (!make_test(src.data, 0, NULL, NULL, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE INDEX ON %1$s USING btree ("%2$s");
        ), w->schema_table, name);
        make_ddl(src.data, SPI_OK_UTILITY);
    }
    pfree(src.data);
}

static void make_hash(const Work *w, const char *value) {
    Datum values[] = {CStringGetTextDatum(value)};
    static Oid argtypes[] = {TEXTOID};
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_index WHERE 0 OPERATOR(pg_catalog.=) indkey[0] AND indrelid OPERATOR(pg_catalog.=) %1$i AND pg_catalog.pg_get_expr(indexprs, indrelid) OPERATOR(pg_catalog.=) $1) AS "test"
    ), w->shared->oid);
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE INDEX ON %1$s USING btree (%2$s);
        ), w->schema_table, value);
        make_ddl(src.data, SPI_OK_UTILITY);
    }
    pfree(src.data);
    pfree((void *)values[0]);
}

static void make_table_comment(const Work *w, const char *value) {
    Datum values[] = {CStringGetTextDatum(value), ObjectIdGetDatum(w->shared->oid)};
    static Oid argtypes[] = {TEXTOID, OIDOID};
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT (SELECT "description" FROM pg_catalog.pg_description WHERE objoid OPERATOR(pg_catalog.=) $2 AND objsubid OPERATOR(pg_catalog.=) 0) IS NOT DISTINCT FROM $1 AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        const char *quote_value = quote_literal_cstr(value);
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            COMMENT ON TABLE %1$s IS %2$s;
        ), w->schema_table, quote_value);
        make_ddl(src.data, SPI_OK_UTILITY);
        if (quote_value != value) pfree((void *)quote_value);
    }
    pfree(src.data);
    pfree((void *)values[0]);
}

static void make_comment(const Work *w, const char *name, const char *value) {
    Datum values[] = {CStringGetTextDatum(value), CStringGetTextDatum(name), ObjectIdGetDatum(w->shared->oid)};
    static Oid argtypes[] = {TEXTOID, TEXTOID, OIDOID};
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT (SELECT "description" FROM pg_catalog.pg_description JOIN pg_catalog.pg_attribute ON attrelid OPERATOR(pg_catalog.=) objoid WHERE attnum OPERATOR(pg_catalog.=) objsubid AND attrelid OPERATOR(pg_catalog.=) $3 AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) $2) IS NOT DISTINCT FROM $1 AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        const char *quote_name = quote_identifier(name);
        const char *quote_value = quote_literal_cstr(value);
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            COMMENT ON COLUMN %1$s.%2$s IS %3$s;
        ), w->schema_table, quote_name, quote_value);
        make_ddl(src.data, SPI_OK_UTILITY);
        if (quote_name != name) pfree((void *)quote_name);
        if (quote_value != value) pfree((void *)quote_value);
    }
    pfree(src.data);
    pfree((void *)values[0]);
    pfree((void *)values[1]);
}

void make_table(const Work *w) {
    Datum values[] = {CStringGetTextDatum(w->shared->schema), CStringGetTextDatum(w->shared->table)};
    static Oid argtypes[] = {TEXTOID, TEXTOID};
    StringInfoData src;
    elog(DEBUG1, "schema_table = %s, schema_type = %s", w->schema_table, w->schema_type);
    set_ps_display_my("table");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT r.rolname::pg_catalog.text AS "owner" FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid OPERATOR(pg_catalog.=) relnamespace JOIN pg_catalog.pg_roles r ON r.oid OPERATOR(pg_catalog.=) relowner WHERE nspname OPERATOR(pg_catalog.=) $1 AND relname OPERATOR(pg_catalog.=) $2 AND relkind OPERATOR(pg_catalog.=) ANY(ARRAY['r', 'p']::"char"[]) AND r.rolname OPERATOR(pg_catalog.<>) current_user AND NOT r.rolsuper
    ));
    make_owner("table", w->schema_table, src.data, countof(argtypes), argtypes, values);
    resetStringInfo(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid OPERATOR(pg_catalog.=) relnamespace WHERE nspname OPERATOR(pg_catalog.=) $1 AND relname OPERATOR(pg_catalog.=) $2 AND relkind OPERATOR(pg_catalog.=) ANY(ARRAY['r', 'p']::"char"[])) AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE TABLE %1$s (
                "id" serial8 PRIMARY KEY,
                "parent" pg_catalog.int8,
                "plan" pg_catalog.timestamptz,
                "start" pg_catalog.timestamptz,
                "stop" pg_catalog.timestamptz,
                "active" pg_catalog.interval,
                "live" pg_catalog.interval,
                "repeat" pg_catalog.interval,
                "timeout" pg_catalog.interval,
                "count" pg_catalog.int4,
                "max" pg_catalog.int4,
                "pid" pg_catalog.int4,
                "state" %2$s,
                "delete" pg_catalog.bool,
                "drift" pg_catalog.bool,
                "header" pg_catalog.bool,
                "save" pg_catalog.bool,
                "string" pg_catalog.bool,
                "delimiter" pg_catalog.char,
                "escape" pg_catalog.char,
                "quote" pg_catalog.char,
                "data" pg_catalog.text,
                "error" pg_catalog.text,
                "group" pg_catalog.text,
                "input" pg_catalog.text,
                "null" pg_catalog.text,
                "output" pg_catalog.text,
                "remote" pg_catalog.text,
                "user" pg_catalog.name
            );
        ), w->schema_table, w->schema_type);
        make_ddl(src.data, SPI_OK_UTILITY);
    }
    resetStringInfo(&src);
    appendStringInfo(&src, SQL(
        SELECT pg_catalog.pg_class.oid FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid OPERATOR(pg_catalog.=) relnamespace WHERE nspname OPERATOR(pg_catalog.=) $1 AND relname OPERATOR(pg_catalog.=) $2 AND relkind OPERATOR(pg_catalog.=) ANY(ARRAY['r', 'p']::"char"[])
    ));
    w->shared->oid = make_oid(src.data, countof(argtypes), argtypes, values, NULL);
    pfree(src.data);
    pfree((void *)values[0]);
    pfree((void *)values[1]);
    make_legacy_hash(w);
    make_column(w, "parent", "pg_catalog.int8");
    make_column(w, "plan", "pg_catalog.timestamptz");
    make_column(w, "start", "pg_catalog.timestamptz");
    make_column(w, "stop", "pg_catalog.timestamptz");
    make_column(w, "active", "pg_catalog.interval");
    make_column(w, "live", "pg_catalog.interval");
    make_column(w, "repeat", "pg_catalog.interval");
    make_column(w, "timeout", "pg_catalog.interval");
    make_column(w, "count", "pg_catalog.int4");
    make_column(w, "max", "pg_catalog.int4");
    make_column(w, "pid", "pg_catalog.int4");
    make_column(w, "state", w->schema_type);
    make_column(w, "delete", "pg_catalog.bool");
    make_column(w, "drift", "pg_catalog.bool");
    make_column(w, "header", "pg_catalog.bool");
    make_column(w, "save", "pg_catalog.bool");
    make_column(w, "string", "pg_catalog.bool");
    make_column(w, "delimiter", "pg_catalog.char");
    make_column(w, "escape", "pg_catalog.char");
    make_column(w, "quote", "pg_catalog.char");
    make_column(w, "data", "pg_catalog.text");
    make_column(w, "error", "pg_catalog.text");
    make_column(w, "group", "pg_catalog.text");
    make_column(w, "input", "pg_catalog.text");
    make_column(w, "null", "pg_catalog.text");
    make_column(w, "output", "pg_catalog.text");
    make_column(w, "remote", "pg_catalog.text");
    make_column(w, "user", "pg_catalog.name");
    make_table_comment(w, "Tasks");
    make_comment(w, "id", "Primary key");
    make_comment(w, "parent", "Parent task id (if exists, like foreign key to id, but without constraint, for performance)");
    make_comment(w, "plan", "Planned date and time of start");
    make_comment(w, "start", "Actual date and time of start");
    make_comment(w, "stop", "Actual date and time of stop");
    make_comment(w, "active", "Positive period after plan time, when task is active for executing");
    make_comment(w, "live", "Non-negative maximum time of live of current background worker process before exit");
    make_comment(w, "repeat", "Non-negative auto repeat tasks interval");
    make_comment(w, "timeout", "Non-negative allowed time for task run");
    make_comment(w, "count", "Non-negative maximum count of tasks, are executed by current background worker process before exit");
    make_comment(w, "max", "Maximum count of concurrently executing tasks in group, negative value means pause between tasks in milliseconds");
    make_comment(w, "pid", "Id of process executing task");
    make_comment(w, "state", "Task state");
    make_comment(w, "delete", "Auto delete task when both output and error are nulls");
    make_comment(w, "drift", "Compute next repeat time by stop time instead by plan time");
    make_comment(w, "header", "Show columns headers in output");
    make_comment(w, "save", "Save session state between tasks");
    make_comment(w, "string", "Quote only strings");
    make_comment(w, "delimiter", "Results columns delimiter");
    make_comment(w, "escape", "Results columns escape");
    make_comment(w, "quote", "Results columns quote");
    make_comment(w, "data", "Some user data");
    make_comment(w, "error", "Catched error");
    make_comment(w, "group", "Task grouping by name");
    make_comment(w, "input", "Sql command(s) to execute");
    make_comment(w, "null", "Null text value representation");
    make_comment(w, "output", "Received result(s)");
    make_comment(w, "remote", "Connect to remote database (if need)");
    make_comment(w, "user", "Role that inserted the task; input is executed as this role");
    make_default(w, "parent", "NULLIF((current_setting('pg_task.id'::text))::bigint, 0)");
    make_default(w, "plan", init_plan());
    make_default(w, "active", "(current_setting('pg_task.active'::text))::interval");
    make_default(w, "live", "(current_setting('pg_task.live'::text))::interval");
    make_default(w, "repeat", "(current_setting('pg_task.repeat'::text))::interval");
    make_default(w, "timeout", "(current_setting('pg_task.timeout'::text))::interval");
    make_default(w, "count", "(current_setting('pg_task.count'::text))::integer");
    make_default(w, "max", "(current_setting('pg_task.max'::text))::integer");
    {
        StringInfoData state;
        initStringInfoMy(&state);
        appendStringInfo(&state, "'PLAN'::%s", w->schema_type);
        make_default(w, "state", state.data);
        pfree(state.data);
    }
    make_default(w, "delete", "(current_setting('pg_task.delete'::text))::boolean");
    make_default(w, "drift", "(current_setting('pg_task.drift'::text))::boolean");
    make_default(w, "header", "(current_setting('pg_task.header'::text))::boolean");
    make_default(w, "save", "(current_setting('pg_task.save'::text))::boolean");
    make_default(w, "string", "(current_setting('pg_task.string'::text))::boolean");
    make_default(w, "delimiter", "(current_setting('pg_task.delimiter'::text))::\"char\"");
    make_default(w, "escape", "(current_setting('pg_task.escape'::text))::\"char\"");
    make_default(w, "quote", "(current_setting('pg_task.quote'::text))::\"char\"");
    make_default(w, "group", "current_setting('pg_task.group'::text)");
    make_default(w, "null", "current_setting('pg_task.null'::text)");
    make_default(w, "user", // as pg_get_expr() deparses it, or before 10 the check never matches and each start redoes the default, waiting for every lock on the table
#if PG_VERSION_NUM >= 100000
        "CURRENT_USER"
#else
        "\"current_user\"()"
#endif
    );
    make_not_null(w, "id", true);
    make_not_null(w, "parent", false);
    make_not_null(w, "plan", true);
    make_not_null(w, "start", false);
    make_not_null(w, "stop", false);
    make_not_null(w, "active", true);
    make_not_null(w, "live", true);
    make_not_null(w, "repeat", true);
    make_not_null(w, "timeout", true);
    make_not_null(w, "count", true);
    make_not_null(w, "max", true);
    make_not_null(w, "pid", false);
    make_not_null(w, "state", true);
    make_not_null(w, "delete", true);
    make_not_null(w, "drift", true);
    make_not_null(w, "header", true);
    make_not_null(w, "save", true);
    make_not_null(w, "string", true);
    make_not_null(w, "delimiter", true);
    make_not_null(w, "escape", true);
    make_not_null(w, "quote", true);
    make_not_null(w, "data", false);
    make_not_null(w, "error", false);
    make_not_null(w, "group", true);
    make_not_null(w, "input", true);
    make_not_null(w, "null", true);
    make_not_null(w, "output", false);
    make_not_null(w, "remote", false);
    make_not_null(w, "user", true);
    make_constraint(w, "active", "> '00:00:00'::interval", "::pg_catalog.interval");
    make_constraint(w, "live", ">= '00:00:00'::interval", "::pg_catalog.interval");
    make_constraint(w, "repeat", ">= '00:00:00'::interval", "::pg_catalog.interval");
    make_constraint(w, "timeout", ">= '00:00:00'::interval", "::pg_catalog.interval");
    make_constraint(w, "count", ">= 0", NULL);
    make_hash(w, "hashtext((\"group\" || COALESCE(remote, ''::text)))");
    make_index(w, "input");
    make_index(w, "parent");
    make_index(w, "plan");
    make_index(w, "state");
    make_wake_up(w);
    make_stop(w);
    make_user_immutable(w);
    make_state_machine(w);
    make_valid(w);
    make_immutable(w, "id"); // the lock of a running task is by its id, which a change of would leave it for work_reset() to run again, and its bookkeeping without the row
    make_immutable(w, "group");
    make_immutable(w, "remote");
    make_immutable(w, "parent");
    make_conditional_immutable(w, "plan");
    make_conditional_immutable(w, "active");
    make_conditional_immutable(w, "live");
    make_conditional_immutable(w, "repeat");
    make_conditional_immutable(w, "timeout");
    make_conditional_immutable(w, "count");
    make_conditional_immutable(w, "max");
    make_conditional_immutable(w, "delete");
    make_conditional_immutable(w, "drift");
    make_conditional_immutable(w, "header");
    make_conditional_immutable(w, "save");
    make_conditional_immutable(w, "string");
    make_conditional_immutable(w, "delimiter");
    make_conditional_immutable(w, "escape");
    make_conditional_immutable(w, "quote");
    make_conditional_immutable(w, "input");
    make_conditional_immutable(w, "null");
    make_conditional_immutable(w, "data");
#if PG_VERSION_NUM >= 90500
    make_policy(w);
#endif
    set_ps_display_my("idle");
}

static void make_enum(const Work *w, const char *name) {
    Datum values[] = {CStringGetTextDatum(w->shared->schema)};
    static Oid argtypes[] = {TEXTOID};
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_enum JOIN pg_catalog.pg_type ON pg_catalog.pg_type.oid OPERATOR(pg_catalog.=) enumtypid JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid OPERATOR(pg_catalog.=) typnamespace WHERE nspname OPERATOR(pg_catalog.=) $1 AND typname OPERATOR(pg_catalog.=) 'state' AND enumlabel OPERATOR(pg_catalog.=) '%s') AS "test"
    ), name);
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            ALTER TYPE %1$s ADD VALUE '%2$s';
        ), w->schema_type, name);
#if PG_VERSION_NUM >= 120000
        make_ddl(src.data, SPI_OK_UTILITY);
#else
        if (!MessageContext) MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext", ALLOCSET_DEFAULT_SIZES);
        SetCurrentStatementStartTimestamp();
        exec_simple_query_my(src.data);
        MemoryContextResetAndDeleteChildren(MessageContext);
#endif
    }
    pfree(src.data);
    pfree((void *)values[0]);
}

void make_type(const Work *w) {
    Datum values[] = {CStringGetTextDatum(w->shared->schema)};
    static Oid argtypes[] = {TEXTOID};
    StringInfoData src;
    set_ps_display_my("type");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT r.rolname::pg_catalog.text AS "owner" FROM pg_catalog.pg_type JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid OPERATOR(pg_catalog.=) typnamespace JOIN pg_catalog.pg_roles r ON r.oid OPERATOR(pg_catalog.=) typowner WHERE nspname OPERATOR(pg_catalog.=) $1 AND typname OPERATOR(pg_catalog.=) 'state' AND r.rolname OPERATOR(pg_catalog.<>) current_user AND NOT r.rolsuper
    ));
    make_owner("type", w->schema_type, src.data, countof(argtypes), argtypes, values);
    resetStringInfo(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_type JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid OPERATOR(pg_catalog.=) typnamespace WHERE nspname OPERATOR(pg_catalog.=) $1 AND typname OPERATOR(pg_catalog.=) 'state') AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE TYPE %s AS ENUM ('PLAN', 'GONE', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP');
        ), w->schema_type);
        make_ddl(src.data, SPI_OK_UTILITY);
    }
    pfree(src.data);
    pfree((void *)values[0]);
    make_enum(w, "PLAN");
    make_enum(w, "GONE");
    make_enum(w, "TAKE");
    make_enum(w, "WORK");
    make_enum(w, "DONE");
    make_enum(w, "FAIL");
    make_enum(w, "STOP");
    set_ps_display_my("idle");
}

void make_user(const Work *w) {
    Datum values[] = {CStringGetTextDatum(w->shared->user)};
    static Oid argtypes[] = {TEXTOID};
    StringInfoData src;
    set_ps_display_my("user");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_roles WHERE rolname OPERATOR(pg_catalog.=) $1) AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE ROLE %s WITH LOGIN;
        ), w->user);
        make_ddl(src.data, SPI_OK_UTILITY);
    }
    pfree(src.data);
    pfree((void *)values[0]);
    set_ps_display_my("idle");
}

void make_data(const Work *w) {
    Datum values[] = {CStringGetTextDatum(w->shared->data)};
    static Oid argtypes[] = {TEXTOID};
    StringInfoData src;
    set_ps_display_my("data");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_database WHERE datname OPERATOR(pg_catalog.=) $1) AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE DATABASE %1$s WITH OWNER = %2$s;
        ), w->data, w->user);
        if (!MessageContext) MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext", ALLOCSET_DEFAULT_SIZES);
        SetCurrentStatementStartTimestamp();
        exec_simple_query_my(src.data);
        MemoryContextResetAndDeleteChildren(MessageContext);
    }
    pfree(src.data);
    pfree((void *)values[0]);
    set_ps_display_my("idle");
}
