DROP TABLE IF EXISTS returning_probe;
CREATE TABLE returning_probe (id serial primary key, val int);
INSERT INTO returning_probe (val) VALUES (1), (2), (3);
DELETE FROM task WHERE "group" IN ('returning_update_no_clause', 'returning_update_with_clause', 'returning_delete_no_match', 'returning_insert', 'returning_insert_on_conflict');
