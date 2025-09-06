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

static Oid make_oid(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls) {
    Oid oid;
    SPI_connect_my(src);
    SPI_execute_with_args_my(src, nargs, argtypes, values, nulls, SPI_OK_SELECT);
    if (SPI_processed != 1) ereport(ERROR, (errmsg("SPI_processed %lu != 1", (long)SPI_processed)));
    oid = DatumGetObjectId(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "oid", false, OIDOID));
    SPI_finish_my();
    return oid;
}

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
            CREATE SCHEMA %s;
        ), w->schema);
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
    }
    pfree(src.data);
    pfree((void *)values[0]);
    set_ps_display_my("idle");
}

static void make_default(const Work *w, const char *name, const char *value) {
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT (SELECT pg_catalog.pg_get_expr(adbin, adrelid) FROM pg_catalog.pg_attribute JOIN pg_catalog.pg_attrdef ON attrelid OPERATOR(pg_catalog.=) adrelid WHERE attnum OPERATOR(pg_catalog.=) adnum AND attrelid OPERATOR(pg_catalog.=) %1$i AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) '%2$s') IS NOT DISTINCT FROM $$%3$s$$ AS "test"
    ), w->shared->oid, name, value);
    if (!make_test(src.data, 0, NULL, NULL, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            ALTER TABLE %1$s ALTER COLUMN "%2$s" SET DEFAULT %3$s;
        ), w->schema_table, name, value);
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
        SELECT (SELECT pg_catalog.pg_get_expr(conbin, conrelid) FROM pg_catalog.pg_constraint JOIN pg_catalog.pg_attribute ON attrelid OPERATOR(pg_catalog.=) conrelid WHERE attnum OPERATOR(pg_catalog.=) conkey[1] AND attrelid OPERATOR(pg_catalog.=) %1$i AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) '%2$s' AND conbin IS NOT NULL) IS NOT DISTINCT FROM $$(%2$s %3$s)$$ AS "test"
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

static void make_column(const Work *w, const char *name, const char *type) {
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT %3$s EXISTS (SELECT * FROM pg_catalog.pg_attribute WHERE attrelid OPERATOR(pg_catalog.=) %1$i AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.=) '%2$s') AS "test"
    ), w->shared->oid, name, type ? "NOT" : "");
    if (make_test(src.data, 0, NULL, NULL, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            ALTER TABLE %1$s %2$s COLUMN "%3$s" %4$s%5$s;
        ), w->schema_table, type ? "ADD" : "DROP", name, type ? "pg_catalog." : "", type ? type : "");
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

static void make_hash(const Work *w, const char *value) {
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_index WHERE 0 OPERATOR(pg_catalog.=) indkey[0] AND indrelid OPERATOR(pg_catalog.=) %1$i AND pg_catalog.pg_get_expr(indexprs, indrelid) OPERATOR(pg_catalog.=) $$%2$s$$) AS "test"
    ), w->shared->oid, value);
    if (!make_test(src.data, 0, NULL, NULL, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE INDEX ON %1$s USING btree (%2$s);
        ), w->schema_table, value);
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
    }
    pfree(src.data);
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
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
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
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
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
        ), w->schema_table, w->schema_type);
        SPI_connect_my(src.data);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        SPI_finish_my();
    }
    resetStringInfo(&src);
    appendStringInfo(&src, SQL(
        SELECT pg_catalog.pg_class.oid FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid OPERATOR(pg_catalog.=) relnamespace WHERE nspname OPERATOR(pg_catalog.=) $1 AND relname OPERATOR(pg_catalog.=) $2 AND relkind OPERATOR(pg_catalog.=) ANY(ARRAY['r', 'p']::"char"[])
    ));
    w->shared->oid = make_oid(src.data, countof(argtypes), argtypes, values, NULL);
    pfree(src.data);
    pfree((void *)values[0]);
    pfree((void *)values[1]);
    make_column(w, "hash", NULL);
    make_column(w, "parent", "int8");
    make_column(w, "plan", "timestamptz");
    make_column(w, "start", "timestamptz");
    make_column(w, "stop", "timestamptz");
    make_column(w, "active", "interval");
    make_column(w, "live", "interval");
    make_column(w, "repeat", "interval");
    make_column(w, "timeout", "interval");
    make_column(w, "count", "int4");
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
    make_default(w, "parent", "NULLIF((current_setting('pg_task.id'::text))::bigint, 0)");
    make_default(w, "plan", init_now());
    make_default(w, "active", "(current_setting('pg_task.active'::text))::interval");
    make_default(w, "live", "(current_setting('pg_task.live'::text))::interval");
    make_default(w, "repeat", "(current_setting('pg_task.repeat'::text))::interval");
    make_default(w, "timeout", "(current_setting('pg_task.timeout'::text))::interval");
    make_default(w, "count", "(current_setting('pg_task.count'::text))::integer");
    make_default(w, "max", "(current_setting('pg_task.max'::text))::integer");
    make_default(w, "state", "'PLAN'::state");
    make_default(w, "delete", "(current_setting('pg_task.delete'::text))::boolean");
    make_default(w, "drift", "(current_setting('pg_task.drift'::text))::boolean");
    make_default(w, "header", "(current_setting('pg_task.header'::text))::boolean");
    make_default(w, "string", "(current_setting('pg_task.string'::text))::boolean");
    make_default(w, "delimiter", "(current_setting('pg_task.delimiter'::text))::\"char\"");
    make_default(w, "escape", "(current_setting('pg_task.escape'::text))::\"char\"");
    make_default(w, "quote", "(current_setting('pg_task.quote'::text))::\"char\"");
    make_default(w, "group", "current_setting('pg_task.group'::text)");
    make_default(w, "null", "current_setting('pg_task.null'::text)");
    make_hash(w, "hashtext((\"group\" || COALESCE(remote, ''::text)))");
    make_index(w, "input");
    make_index(w, "parent");
    make_index(w, "plan");
    make_index(w, "state");
    make_wake_up(w);
    set_ps_display_my("idle");
}

static void make_enum(const Work *w, const char *name) {
    Datum values[] = {CStringGetTextDatum(w->shared->schema)};
    static Oid argtypes[] = {TEXTOID};
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_enum JOIN pg_catalog.pg_type ON pg_catalog.pg_type.oid OPERATOR(pg_catalog.=) enumtypid JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid OPERATOR(pg_catalog.=) typnamespace WHERE nspname OPERATOR(pg_catalog.=) $1 AND enumlabel OPERATOR(pg_catalog.=) '%s') AS "test"
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
        SELECT EXISTS (SELECT * FROM pg_catalog.pg_type JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid OPERATOR(pg_catalog.=) typnamespace WHERE nspname OPERATOR(pg_catalog.=) $1 AND typname OPERATOR(pg_catalog.=) 'state') AS "test"
    ));
    if (!make_test(src.data, countof(argtypes), argtypes, values, NULL)) {
        resetStringInfo(&src);
        appendStringInfo(&src, SQL(
            CREATE TYPE %s AS ENUM ('PLAN', 'GONE', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP');
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
            CREATE ROLE %s WITH LOGIN;
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
