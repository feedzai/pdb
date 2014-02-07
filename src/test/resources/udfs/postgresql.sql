CREATE OR REPLACE FUNCTION GetOne()
RETURNS INTEGER
AS 'SELECT 1;'
LANGUAGE SQL;

--
-- Query:
-- SELECT GetOne();
--
-- Output:
-- 1
--

DROP SCHEMA IF EXISTS myschema CASCADE;
CREATE SCHEMA myschema;

CREATE OR REPLACE FUNCTION myschema.TimesTwo(INTEGER)
RETURNS INTEGER
AS 'SELECT $1 * 2;'
LANGUAGE SQL;

--
-- Query:
-- SELECT myschema.TimesTwo(10);
--
-- Output:
-- 20
--
