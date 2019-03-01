DROP TABLE if exists task;
DROP TYPE if exists state;
CREATE TYPE state AS ENUM (
    'QUEUE',
    'ASSIGN',
    'WORK',
    'DONE',
    'FAIL',
);
ALTER TYPE state OWNER TO pgs;
CREATE TABLE task (
    id bigserial not null primary key,
    dt timestamp not null default now(),
    start timestamp,
    stop timestamp,
    request text NOT NULL,
    response text,
    state state not null default 'QUEUE',
    parent bigint,
    CONSTRAINT task_parent_fkey FOREIGN KEY (parent) REFERENCES task (id) MATCH SIMPLE ON UPDATE cascade ON DELETE cascade
);
ALTER TABLE task OWNER to pgs;
create or replace function notify_trigger() returns trigger language plpgsql as $body$ begin
    perform pg_notify(case when TG_NARGS > 0 then TG_ARGV[0] else 'task' end, (select to_json(i) from (select 
        tg_name,
        tg_when,
        tg_level,
        tg_op,
        tg_relid,
        tg_table_name,
        tg_table_schema,
        tg_nargs,
        tg_argv,
        transaction_timestamp() as transaction_timestamp,
        clock_timestamp() as clock_timestamp,
        case when tg_op in ('INSERT', 'UPDATE') then new else null end as new,
        case when tg_op in ('UPDATE', 'DELETE') then old else null end as old
    ) as i)::text);
    return case when TG_OP = 'DELETE' then old else new end;
end;$body$;alter function notify_trigger() owner to pgs;
create trigger notify_before_trigger before insert or update or delete on task for each row execute procedure notify_trigger();
create trigger notify_after_trigger after insert or update or delete on task for each row execute procedure notify_trigger();
