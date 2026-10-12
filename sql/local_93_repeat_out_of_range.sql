-- a repeat whose next plan, with its intervals, the check of the table finds out of range, its timeout of hundreds of thousands of years taking the task's own to just within it, is left uninserted, with a warning, the task done, rather than fail its bookkeeping, outside any error handling, leaving the task in WORK, to run again on every reset
DELETE FROM task WHERE "group" = 'repeat_out_of_range';
INSERT INTO task ("group", input, repeat, active, live, timeout) VALUES ('repeat_out_of_range', 'SELECT 1', '1 day', '1 hour', '0 sec', '294276-12-31 12:00 UTC'::timestamptz - '1 hour'::interval - '1 day'::interval - now());
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'repeat_out_of_range' AND state = 'DONE') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of group repeat_out_of_range to be done'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pg_sleep(1);
-- done, its repeat left out, from 9.6 on, where adding to a timestamp checks its range, or inserted, before
SELECT state FROM task WHERE "group" = 'repeat_out_of_range' AND parent IS NULL;
DELETE FROM task WHERE "group" = 'repeat_out_of_range';
