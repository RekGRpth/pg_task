-- the statements of an input are read by the settings of the lexer it starts with, backslash_quote and, before 19, standard_conforming_strings, as local mode and remote mode read them all at once, not each by those the statements before it set, which in spi mode reads each anew, its text sliced by the first reading (before 19 a string ending in a backslash ends where the first reading has it); what the statements run, SHOW or the body of a DO, by those set
DELETE FROM task WHERE "group" = 'lexer_settings';
INSERT INTO task (id, "group", input) VALUES
    (-741, 'lexer_settings', $q$SET backslash_quote = off; SELECT E'a\'b' AS v$q$),
    (-742, 'lexer_settings', $q$SET standard_conforming_strings = off; SELECT 'a\\b' AS v$q$),
    (-743, 'lexer_settings', $q$SET standard_conforming_strings = off; SELECT 'x\' AS v; SELECT 1 AS w; --'$q$),
    (-744, 'lexer_settings', $q$SET backslash_quote = off; SHOW backslash_quote$q$),
    (-745, 'lexer_settings', $q$SET backslash_quote = off; DO $$BEGIN PERFORM E'a\'b'; END$$$q$);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'lexer_settings' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of group lexer_settings to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT -id AS n, CASE id
    WHEN -741 THEN state = 'DONE' AND output = 'SET' || chr(10) || 'a''b'
    WHEN -742 THEN current_setting('server_version_num')::int >= 190000 OR (state = 'DONE' AND output = 'SET' || chr(10) || 'a\\b')
    WHEN -743 THEN current_setting('server_version_num')::int >= 190000 OR (state = 'DONE' AND output = 'SET' || chr(10) || 'x\' || chr(10) || '1')
    WHEN -744 THEN state = 'DONE' AND output = 'SET' || chr(10) || 'off'
    WHEN -745 THEN state = 'FAIL' AND output = 'SET' AND error LIKE 'ERROR:  unsafe use of % in a string literal%'
END AS ok FROM task WHERE "group" = 'lexer_settings' ORDER BY id DESC;
DELETE FROM task WHERE "group" = 'lexer_settings';
