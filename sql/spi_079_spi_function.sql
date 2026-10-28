-- in spi mode, a function a query calls that runs queries of its own through SPI, one of PL/pgSQL say, runs, of a SELECT, of an EXECUTE and of a FETCH, which spi mode runs by the executor itself, as local mode does, rather than failing to connect to SPI before 10
DELETE FROM task WHERE "group" LIKE 'spi_function%';
CREATE FUNCTION spi_function(i int) RETURNS int LANGUAGE plpgsql AS $function$BEGIN
    RETURN (SELECT i + 1);
END;$function$;
INSERT INTO task (id, "group", input) VALUES
    (-791, 'spi_function', 'SELECT public.spi_function(1) AS a'),
    (-792, 'spi_function', 'PREPARE spi_function_p AS SELECT public.spi_function(2) AS a; EXECUTE spi_function_p'),
    (-793, 'spi_function', 'DECLARE spi_function_c CURSOR WITH HOLD FOR SELECT public.spi_function(3) AS a; FETCH spi_function_c; CLOSE spi_function_c'),
    (-794, 'spi_function', 'SELECT public.spi_function(4) / 0 AS a');
-- and one failing in it, the next task the same worker takes, by live, to connect to SPI still, its connection not left pushed
INSERT INTO task (id, "group", input, live) VALUES
    (-796, 'spi_function_live', 'SELECT public.spi_function(5) / 0 AS a', '1 min'),
    (-795, 'spi_function_live', 'SELECT public.spi_function(6) AS a', '1 min');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" LIKE 'spi_function%' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of group spi_function to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT -id AS n, state, output, split_part(error, E'\n', 1) AS error FROM task WHERE "group" LIKE 'spi_function%' ORDER BY id DESC;
SELECT count(DISTINCT pid) = 1 AS same_worker FROM task WHERE "group" = 'spi_function_live';
DELETE FROM task WHERE "group" LIKE 'spi_function%';
DROP FUNCTION spi_function(int);
