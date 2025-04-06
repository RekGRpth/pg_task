#include "include.h"

#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_collation.h>
#include <libpq/libpq-be.h>
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

#if PG_VERSION_NUM >= 120000
#include <access/relation.h>
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

static bool make_test(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls) {
    bool test;
    SPI_connect_my(src);
    SPI_execute_with_args_my(src, nargs, argtypes, values, nulls, SPI_OK_SELECT);
    if (SPI_processed != 1) ereport(ERROR, (errmsg("SPI_processed %lu != 1", (long)SPI_processed)));
    test = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "test", false, BOOLOID));
    SPI_finish_my();
    return test;
}

void make_schema(const Work *w) {
    Datum values[] = {CStringGetTextDatum(w->shared->schema)};
    static Oid argtypes[] = {TEXTOID};
    StringInfoData src;
    set_ps_display_my("schema");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_namespace WHERE nspname OPERATOR(pg_catalog.=) $1) AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE SCHEMA %1$s;
        ), w->schema);
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
    }
    pfree(src.data);
    pfree((void *)values[0]);
    set_ps_display_my("idle");
}

static void make_default(const Work *w, const char *name, const char *type, const char *catalog) {
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT (SELECT pg_catalog.pg_get_expr(adbin, adrelid) FROM pg_catalog.pg_attribute JOIN pg_catalog.pg_attrdef ON attrelid OPERATOR(pg_catalog.=) adrelid WHERE attnum OPERATOR(pg_catalog.=) adnum AND attrelid OPERATOR(pg_catalog.=) %1$i AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) '%2$s') IS NOT DISTINCT FROM $$%3$scurrent_setting('pg_task.%2$s'::text)%4$s%5$s$$ AS "test"
    ), w->shared->oid, name, type ? "(" : "", type ? ")" : "", type ? type : "");
    if (!make_test(src.data, 0, NULL, NULL, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            ALTER TABLE %1$s ALTER COLUMN "%2$s" SET DEFAULT (pg_catalog.current_setting('pg_task.%2$s'))::pg_catalog.%3$s;
        ), w->schema_table, name, catalog);
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
    }
    pfree(src.data);
}

static void make_constraint(const Work *w, const char *name, const char *value, const char *type) {
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT (SELECT pg_catalog.pg_get_expr(conbin, conrelid) FROM pg_catalog.pg_constraint JOIN pg_catalog.pg_attribute ON attrelid OPERATOR(pg_catalog.=) conrelid WHERE attnum OPERATOR(pg_catalog.=) conkey[1] AND attrelid OPERATOR(pg_catalog.=) %1$i AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) '%2$s') IS NOT DISTINCT FROM $$(%2$s %3$s)$$ AS "test"
    ), w->shared->oid, name, value);
    if (!make_test(src.data, 0, NULL, NULL, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            ALTER TABLE %1$s ADD CHECK ("%2$s" %3$s%4$s);
        ), w->schema_table, name, value, type ? type : "");
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
    }
    pfree(src.data);
}

static void make_function(const Work *w, const char *name, const char *source) {
    Datum values[] = {CStringGetTextDatum(name), CStringGetTextDatum(w->shared->schema), CStringGetTextDatum(source)};
    static Oid argtypes[] = {TEXTOID, TEXTOID, TEXTOID};
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT (SELECT prosrc FROM pg_catalog.pg_proc JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) pronamespace WHERE proname OPERATOR(pg_catalog.=) $1 AND nspname OPERATOR(pg_catalog.=) $2) IS NOT DISTINCT FROM $3 AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        const char *quote = quote_identifier(name);
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE OR REPLACE FUNCTION %1$s.%2$s() RETURNS TRIGGER SET search_path = pg_catalog, pg_temp AS $function$%3$s$function$ LANGUAGE plpgsql;
        ), w->schema, quote, source);
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
        if (quote != name) pfree((void *)quote);
    }
    pfree(src.data);
    pfree((void *)values[0]);
    pfree((void *)values[1]);
    pfree((void *)values[2]);
}

static void make_trigger(const Work *w, const char *name, const char *when, const char *each) {
    Datum values[] = {CStringGetTextDatum(name), ObjectIdGetDatum(w->shared->oid)};
    static Oid argtypes[] = {TEXTOID, OIDOID};
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_trigger WHERE tgname OPERATOR(pg_catalog.=) $1 AND tgrelid OPERATOR(pg_catalog.=) $2) AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        const char *quote = quote_identifier(name);
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE TRIGGER %1$s %2$s ON %3$s FOR EACH %4$s EXECUTE PROCEDURE %5$s.%1$s();
        ), quote, when, w->schema_table, each, w->schema);
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
        if (quote != name) pfree((void *)quote);
    }
    pfree(src.data);
    pfree((void *)values[0]);
}

static void make_wake_up(const Work *w) {
    StringInfoData name;
    StringInfoData source;
    initStringInfoMy(&name);
    appendStringInfo(&name, "%s_wake_up", w->shared->table);
    initStringInfoMy(&source);
    appendStringInfo(&source, SQL(
        BEGIN
            PERFORM pg_catalog.pg_cancel_backend(pid) FROM "pg_catalog"."pg_locks" WHERE "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 3 AND "database" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_database" WHERE "datname" OPERATOR(pg_catalog.=) current_catalog) AND "objid" OPERATOR(pg_catalog.=) %1$i;
            RETURN %2$s;
        END;
    ), w->shared->hash,
#ifdef GP_VERSION_NUM
"NEW"
#else
"NULL"
#endif
    );
    make_function(w, name.data, source.data);
    make_trigger(w, name.data, "AFTER INSERT",
#ifdef GP_VERSION_NUM
        "ROW"
#else
        "STATEMENT"
#endif
    );
    pfree(name.data);
    pfree(source.data);
}

#if PG_VERSION_NUM < 120000
static void make_hash(const Work *w) {
    StringInfoData name;
    StringInfoData source;
    initStringInfoMy(&name);
    appendStringInfo(&name, "%s_hash_generate", w->shared->table);
    initStringInfoMy(&source);
    appendStringInfo(&source, SQL(
        BEGIN
            IF tg_op OPERATOR(pg_catalog.=) 'INSERT' OR (NEW.group, NEW.remote) IS DISTINCT FROM (OLD.group, OLD.remote) THEN
                NEW.hash = pg_catalog.hashtext(NEW.group OPERATOR(pg_catalog.||) COALESCE(NEW.remote, '%s'));
            END IF;
            RETURN NEW;
        END;
    ), "");
    make_function(w, name.data, source.data);
    make_trigger(w, name.data, "BEFORE INSERT OR UPDATE", "ROW");
    pfree(name.data);
    pfree(source.data);
}
#endif

static void make_column(const Work *w, const char *name, const char *type) {
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_attribute WHERE attrelid OPERATOR(pg_catalog.=) %1$i AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) '%2$s') AS "test"
    ), w->shared->oid, name);
    if (!make_test(src.data, 0, NULL, NULL, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            ALTER TABLE %1$s ADD COLUMN "%2$s" pg_catalog.%3$s;
        ), w->schema_table, name, type);
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
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
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
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
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
    }
    pfree(src.data);
}

void make_table(const Work *w) {
    List *names = stringToQualifiedNameListMy(w->schema_table);
    const RangeVar *rangevar = makeRangeVarFromNameList(names);
    StringInfoData src, hash;
    elog(DEBUG1, "schema_table = %s, schema_type = %s", w->schema_table, w->schema_type);
    set_ps_display_my("table");
    initStringInfoMy(&hash);
#if PG_VERSION_NUM >= 120000
    appendStringInfo(&hash, SQL(GENERATED ALWAYS AS (pg_catalog.hashtext("group" OPERATOR(pg_catalog.||) COALESCE("remote", '%s'))) STORED), "");
#endif
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        CREATE TABLE %1$s (
            "id" serial8 PRIMARY KEY,
            "parent" pg_catalog.int8 DEFAULT NULLIF(pg_catalog.current_setting('pg_task.id')::pg_catalog.int8, 0),
            "plan" pg_catalog.timestamptz DEFAULT CURRENT_TIMESTAMP,
            "start" pg_catalog.timestamptz,
            "stop" pg_catalog.timestamptz,
            "active" pg_catalog.interval,
            "live" pg_catalog.interval,
            "repeat" pg_catalog.interval,
            "timeout" pg_catalog.interval,
            "count" pg_catalog.int4,
            "hash" pg_catalog.int4 %3$s,
            "max" pg_catalog.int4,
            "pid" pg_catalog.int4,
            "state" %2$s DEFAULT 'PLAN',
            "delete" pg_catalog.bool,
            "drift" pg_catalog.bool,
            "header" pg_catalog.bool,
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
            "remote" pg_catalog.text
        );
        COMMENT ON TABLE %1$s IS 'Tasks';
        COMMENT ON COLUMN %1$s."id" IS 'Primary key';
        COMMENT ON COLUMN %1$s."parent" IS 'Parent task id (if exists, like foreign key to id, but without constraint, for performance)';
        COMMENT ON COLUMN %1$s."plan" IS 'Planned date and time of start';
        COMMENT ON COLUMN %1$s."start" IS 'Actual date and time of start';
        COMMENT ON COLUMN %1$s."stop" IS 'Actual date and time of stop';
        COMMENT ON COLUMN %1$s."active" IS 'Positive period after plan time, when task is active for executing';
        COMMENT ON COLUMN %1$s."live" IS 'Non-negative maximum time of live of current background worker process before exit';
        COMMENT ON COLUMN %1$s."repeat" IS 'Non-negative auto repeat tasks interval';
        COMMENT ON COLUMN %1$s."timeout" IS 'Non-negative allowed time for task run';
        COMMENT ON COLUMN %1$s."count" IS 'Non-negative maximum count of tasks, are executed by current background worker process before exit';
        COMMENT ON COLUMN %1$s."hash" IS 'Hash for identifying tasks group';
        COMMENT ON COLUMN %1$s."max" IS 'Maximum count of concurrently executing tasks in group, negative value means pause between tasks in milliseconds';
        COMMENT ON COLUMN %1$s."pid" IS 'Id of process executing task';
        COMMENT ON COLUMN %1$s."state" IS 'Task state';
        COMMENT ON COLUMN %1$s."delete" IS 'Auto delete task when both output and error are nulls';
        COMMENT ON COLUMN %1$s."drift" IS 'Compute next repeat time by stop time instead by plan time';
        COMMENT ON COLUMN %1$s."header" IS 'Show columns headers in output';
        COMMENT ON COLUMN %1$s."string" IS 'Quote only strings';
        COMMENT ON COLUMN %1$s."delimiter" IS 'Results columns delimiter';
        COMMENT ON COLUMN %1$s."escape" IS 'Results columns escape';
        COMMENT ON COLUMN %1$s."quote" IS 'Results columns quote';
        COMMENT ON COLUMN %1$s."data" IS 'Some user data';
        COMMENT ON COLUMN %1$s."error" IS 'Catched error';
        COMMENT ON COLUMN %1$s."group" IS 'Task grouping by name';
        COMMENT ON COLUMN %1$s."input" IS 'Sql command(s) to execute';
        COMMENT ON COLUMN %1$s."null" IS 'Null text value representation';
        COMMENT ON COLUMN %1$s."output" IS 'Received result(s)';
        COMMENT ON COLUMN %1$s."remote" IS 'Connect to remote database (if need)';
    ), w->schema_table, w->schema_type,
#if PG_VERSION_NUM >= 120000
        hash.data
#else
        ""
#endif
    );
    SPI_connect_my(src.data);
    if (!OidIsValid(RangeVarGetRelid(rangevar, NoLock, true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    w->shared->oid = RangeVarGetRelid(rangevar, NoLock, false);
    SPI_finish_my();
    pfree((void *)rangevar);
    list_free_deep(names);
    resetStringInfo(&src);
    pfree(hash.data);
    pfree(src.data);
    make_column(w, "parent", "int8");
    make_column(w, "plan", "timestamptz");
    make_column(w, "start", "timestamptz");
    make_column(w, "stop", "timestamptz");
    make_column(w, "active", "interval");
    make_column(w, "live", "interval");
    make_column(w, "repeat", "interval");
    make_column(w, "timeout", "interval");
    make_column(w, "count", "int4");
    make_column(w, "hash", "int4");
    make_column(w, "max", "int4");
    make_column(w, "pid", "int4");
    make_column(w, "state", w->schema_type);
    make_column(w, "delete", "bool");
    make_column(w, "drift", "bool");
    make_column(w, "header", "bool");
    make_column(w, "string", "bool");
    make_column(w, "delimiter", "char");
    make_column(w, "escape", "char");
    make_column(w, "quote", "char");
    make_column(w, "data", "text");
    make_column(w, "error", "text");
    make_column(w, "group", "text");
    make_column(w, "input", "text");
    make_column(w, "null", "text");
    make_column(w, "output", "text");
    make_column(w, "remote", "text");
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
    make_not_null(w, "hash", true);
    make_not_null(w, "max", true);
    make_not_null(w, "pid", false);
    make_not_null(w, "state", true);
    make_not_null(w, "delete", true);
    make_not_null(w, "drift", true);
    make_not_null(w, "header", true);
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
    make_constraint(w, "active", "> '00:00:00'::interval", "::pg_catalog.interval");
    make_constraint(w, "live", ">= '00:00:00'::interval", "::pg_catalog.interval");
    make_constraint(w, "repeat", ">= '00:00:00'::interval", "::pg_catalog.interval");
    make_constraint(w, "timeout", ">= '00:00:00'::interval", "::pg_catalog.interval");
    make_constraint(w, "count", ">= 0", NULL);
    make_default(w, "active", "::interval", "interval");
    make_default(w, "live", "::interval", "interval");
    make_default(w, "repeat", "::interval", "interval");
    make_default(w, "timeout", "::interval", "interval");
    make_default(w, "count", "::integer", "int4");
    make_default(w, "max", "::integer", "int4");
    make_default(w, "delete", "::boolean", "bool");
    make_default(w, "drift", "::boolean", "bool");
    make_default(w, "header", "::boolean", "bool");
    make_default(w, "string", "::boolean", "bool");
    make_default(w, "delimiter", "::\"char\"", "char");
    make_default(w, "escape", "::\"char\"", "char");
    make_default(w, "quote", "::\"char\"", "char");
    make_default(w, "group", NULL, "text");
    make_default(w, "null", NULL, "text");
    make_index(w, "hash");
    make_index(w, "input");
    make_index(w, "parent");
    make_index(w, "plan");
    make_index(w, "state");
    make_wake_up(w);
#if PG_VERSION_NUM < 120000
    make_hash(w);
#endif
    set_ps_display_my("idle");
}

static void make_enum(const Work *w, const char *name) {
    Datum values[] = {CStringGetTextDatum(w->shared->schema)};
    static Oid argtypes[] = {TEXTOID};
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_enum JOIN pg_catalog.pg_type ON pg_catalog.pg_type.oid OPERATOR(pg_catalog.=) enumtypid JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid OPERATOR(pg_catalog.=) typnamespace WHERE nspname OPERATOR(pg_catalog.=) $1 AND enumlabel OPERATOR(pg_catalog.=) '%1$s') AS "test"
    ), name);
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            ALTER TYPE %1$s ADD VALUE '%2$s';
        ), w->schema_type, name);
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
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
        SELECT  EXISTS (SELECT * FROM pg_catalog.pg_type JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid OPERATOR(pg_catalog.=) typnamespace WHERE nspname OPERATOR(pg_catalog.=) $1 AND typname OPERATOR(pg_catalog.=) 'state') AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE TYPE %1$s AS ENUM ('PLAN', 'GONE', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP');
        ), w->schema_type);
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
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
            CREATE ROLE %1$s WITH LOGIN;
        ), w->user);
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
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
        ), w->user, w->user);
        if (!MessageContext) MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext", ALLOCSET_DEFAULT_SIZES);
        SetCurrentStatementStartTimestamp();
        exec_simple_query_my(src.data);
        MemoryContextResetAndDeleteChildren(MessageContext);
    }
    pfree(src.data);
    pfree((void *)values[0]);
    set_ps_display_my("idle");
}
