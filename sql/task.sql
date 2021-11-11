\unset ECHO
\i test_setup.sql

SELECT plan(1);
SELECT pass('W00t!');
SELECT * FROM finish();
ROLLBACK;