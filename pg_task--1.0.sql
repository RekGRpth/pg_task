-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_task" to load this file. \quit

CREATE TYPE "state" AS ENUM ('PLAN', 'TAKE', 'WORK', 'DONE', 'STOP');

DO $do$BEGIN
    EXECUTE FORMAT($format$
        CREATE TABLE %1$I (
            "id" bigserial NOT NULL PRIMARY KEY,
            "parent" bigint DEFAULT NULLIF(current_setting('pg_task.id')::bigint, 0),
            "plan" timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "start" timestamp with time zone,
            "stop" timestamp with time zone,
            "active" interval NOT NULL DEFAULT current_setting('pg_task.active')::interval CHECK ("active" > '0 sec'::interval),
            "live" interval NOT NULL DEFAULT current_setting('pg_task.default_live')::interval CHECK ("live" >= '0 sec'::interval),
            "repeat" interval NOT NULL DEFAULT current_setting('pg_task.default_repeat')::interval CHECK ("repeat" >= '0 sec'::interval),
            "timeout" interval NOT NULL DEFAULT current_setting('pg_task.timeout')::interval CHECK ("timeout" >= '0 sec'::interval),
            "count" int NOT NULL DEFAULT current_setting('pg_task.count')::int CHECK ("count" >= 0),
            "hash" int NOT NULL %2$s,
            "max" int NOT NULL DEFAULT current_setting('pg_task.max')::int,
            "pid" int,
            "state" "state" NOT NULL DEFAULT 'PLAN',
            "delete" bool NOT NULL DEFAULT current_setting('pg_task.delete')::bool,
            "drift" bool NOT NULL DEFAULT current_setting('pg_task.drift')::bool,
            "header" bool NOT NULL DEFAULT current_setting('pg_task.header')::bool,
            "string" bool NOT NULL DEFAULT current_setting('pg_task.string')::bool,
            "delimiter" "char" NOT NULL DEFAULT current_setting('pg_task.default_delimiter')::"char",
            "escape" "char" NOT NULL DEFAULT current_setting('pg_task.default_escape')::"char",
            "quote" "char" NOT NULL DEFAULT current_setting('pg_task.default_quote')::"char",
            "error" text,
            "group" text NOT NULL DEFAULT current_setting('pg_task.default_group'),
            "input" text NOT NULL,
            "null" text NOT NULL DEFAULT current_setting('pg_task.default_null'),
            "output" text,
            "remote" text
        );
        CREATE INDEX ON %1$I USING btree ("hash");
        CREATE INDEX ON %1$I USING btree ("input");
        CREATE INDEX ON %1$I USING btree ("parent");
        CREATE INDEX ON %1$I USING btree ("plan");
        CREATE INDEX ON %1$I USING btree ("state");
        SELECT pg_catalog.pg_extension_config_dump(%1$L, '');
        ALTER DATABASE %3$I SET "pg_task.data" TO %3$L;
        ALTER DATABASE %3$I SET "pg_task.schema" TO %5$L;
        ALTER DATABASE %3$I SET "pg_task.table" TO %1$L;
        ALTER DATABASE %3$I SET "pg_task.user" TO %4$L;
    $format$,
        current_setting('pg_task.table'),
        CASE WHEN current_setting('server_version_num')::int >= 120000 THEN $text$GENERATED ALWAYS AS (hashtext("group"||COALESCE("remote", ''))) STORED$text$ ELSE '' END,
        current_catalog,
        current_user,
        current_schema
    );
    IF current_setting('server_version_num')::int < 120000 THEN
        EXECUTE FORMAT($format$
            CREATE FUNCTION %2$I() RETURNS TRIGGER AS $function$BEGIN
                IF tg_op = 'INSERT' OR (new.group, new.remote) IS DISTINCT FROM (old.group, old.remote) THEN
                    new.hash = hashtext(new.group||COALESCE(new.remote, ''));
                END IF;
                RETURN new;
            END;$function$ LANGUAGE plpgsql;
            CREATE TRIGGER hash_generate BEFORE INSERT OR UPDATE ON %1$I FOR EACH ROW EXECUTE PROCEDURE %2$I();
        $format$,
            current_setting('pg_task.table'),
            current_setting('pg_task.table')||'_hash_generate',
        );
    END IF;
END;$do$ LANGUAGE plpgsql;
