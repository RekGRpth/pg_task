-- an error in a parameter of EXECUTE, one of several statements of the input, is at its position within the whole input, in characters, as local mode and remote mode report it, not within the text of that statement alone
DELETE FROM task WHERE "group" = 'execute_error_position';
INSERT INTO task (id, "group", input) VALUES
    (-1041, 'execute_error_position', $q$PREPARE eep1(int) AS SELECT $1;   EXECUTE eep1('x')$q$),
    (-1042, 'execute_error_position', $q$SELECT 'ёё' AS a; PREPARE eep2(int) AS SELECT $1; EXECUTE eep2('y')$q$);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'execute_error_position' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of group execute_error_position to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
-- the wording of the error by the version, and its position in characters of the encoding of the database, a byte each in SQL_ASCII: that of the parameter in the input, counted alike
SELECT -id AS n, state, split_part(error, E'\n', 1) LIKE 'ERROR:  invalid input syntax for %integer: "_" at character %' AS invalid, substring(split_part(error, E'\n', 1) from 'at character ([0-9]+)$')::int = strpos(input, $$('$$) + 1 AS at_parameter FROM task WHERE "group" = 'execute_error_position' ORDER BY id DESC;
DELETE FROM task WHERE "group" = 'execute_error_position';
