-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_task" to load this file. \quit


DO $do$BEGIN
    EXECUTE FORMAT($format$
        CREATE TYPE %1$I."state" AS ENUM ('PLAN', 'TAKE', 'WORK', 'DONE', 'STOP');
        ALTER TYPE %1$I."state" owner TO %4$I;
        CREATE TABLE %1$I.%2$I (
            "id" bigserial NOT NULL PRIMARY KEY,
            "parent" bigint DEFAULT NULLIF(current_setting('pg_task.id')::bigint, 0),
            "plan" timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "start" timestamp with time zone,
            "stop" timestamp with time zone,
            "active" interval NOT NULL DEFAULT current_setting('pg_task.active')::interval CHECK ("active" > '0 sec'::interval),
            "live" interval NOT NULL DEFAULT current_setting('pg_task.live')::interval CHECK ("live" >= '0 sec'::interval),
            "repeat" interval NOT NULL DEFAULT current_setting('pg_task.repeat')::interval CHECK ("repeat" >= '0 sec'::interval),
            "timeout" interval NOT NULL DEFAULT current_setting('pg_task.timeout')::interval CHECK ("timeout" >= '0 sec'::interval),
            "count" int NOT NULL DEFAULT current_setting('pg_task.count')::int CHECK ("count" >= 0),
            "hash" int NOT NULL %3$s,
            "max" int NOT NULL DEFAULT current_setting('pg_task.max')::int,
            "pid" int,
            "state" %1$I."state" NOT NULL DEFAULT 'PLAN',
            "delete" bool NOT NULL DEFAULT current_setting('pg_task.delete')::bool,
            "drift" bool NOT NULL DEFAULT current_setting('pg_task.drift')::bool,
            "header" bool NOT NULL DEFAULT current_setting('pg_task.header')::bool,
            "string" bool NOT NULL DEFAULT current_setting('pg_task.string')::bool,
            "delimiter" "char" NOT NULL DEFAULT current_setting('pg_task.delimiter')::"char",
            "escape" "char" NOT NULL DEFAULT current_setting('pg_task.escape')::"char",
            "quote" "char" NOT NULL DEFAULT current_setting('pg_task.quote')::"char",
            "data" text,
            "error" text,
            "group" text NOT NULL DEFAULT current_setting('pg_task.group'),
            "input" text NOT NULL,
            "null" text NOT NULL DEFAULT current_setting('pg_task.null'),
            "output" text,
            "remote" text
        );
        ALTER TABLE %1$I.%2$I owner TO %4$I;
        CREATE INDEX ON %1$I.%2$I USING btree ("hash");
        CREATE INDEX ON %1$I.%2$I USING btree ("input");
        CREATE INDEX ON %1$I.%2$I USING btree ("parent");
        CREATE INDEX ON %1$I.%2$I USING btree ("plan");
        CREATE INDEX ON %1$I.%2$I USING btree ("state");
        SELECT pg_catalog.pg_extension_config_dump('%1$I.%2$I', '');
        ALTER DATABASE %5$I SET "pg_task.reset" TO %6$L;
        ALTER DATABASE %5$I SET "pg_task.schema" TO %1$L;
        ALTER DATABASE %5$I SET "pg_task.sleep" TO %7$L;
        ALTER DATABASE %5$I SET "pg_task.table" TO %2$L;
    $format$,
        current_schema,
        current_setting('pg_task.table'),
        CASE WHEN current_setting('server_version_num')::int >= 120000 THEN $text$GENERATED ALWAYS AS (hashtext("group"||COALESCE("remote", ''))) STORED$text$ ELSE '' END,
        (SELECT rolname FROM pg_database INNER JOIN pg_roles ON pg_roles.oid = datdba WHERE datname = current_catalog),
        current_catalog,
        current_setting('pg_task.reset'),
        current_setting('pg_task.sleep')
    );
    IF current_setting('server_version_num')::int < 120000 THEN
        EXECUTE FORMAT($format$
            CREATE FUNCTION %1$I.%2$I() RETURNS TRIGGER AS $function$BEGIN
                IF tg_op = 'INSERT' OR (new.group, new.remote) IS DISTINCT FROM (old.group, old.remote) THEN
                    new.hash = hashtext(new.group||COALESCE(new.remote, ''));
                END IF;
                RETURN new;
            END;$function$ LANGUAGE plpgsql;
            CREATE TRIGGER hash_generate BEFORE INSERT OR UPDATE ON %1$I.%2$I FOR EACH ROW EXECUTE PROCEDURE %1$I.%3$I();
        $format$,
            current_schema,
            current_setting('pg_task.table'),
            current_setting('pg_task.table')||'_hash_generate',
        );
    END IF;
END;$do$ LANGUAGE plpgsql;

CREATE FUNCTION pg_task_init_conf() RETURNS void AS 'MODULE_PATHNAME', 'pg_task_init_conf' LANGUAGE 'c';

CREATE FUNCTION pg_task_fini_conf() RETURNS void AS $function$BEGIN
    EXECUTE FORMAT($format$
        ALTER DATABASE %1$I RESET "pg_task.reset";
        ALTER DATABASE %1$I RESET "pg_task.schema";
        ALTER DATABASE %1$I RESET "pg_task.sleep";
        ALTER DATABASE %1$I RESET "pg_task.table";
    $format$, current_catalog);
END;$function$ LANGUAGE plpgsql;
