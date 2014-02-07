DROP FUNCTION IF EXISTS GetOne;

CREATE FUNCTION GetOne()
RETURNS INTEGER
RETURN 1;

--
-- Query:
-- SELECT GetOne();
--
-- Output:
-- 1
--

DROP SCHEMA IF EXISTS myschema;
CREATE SCHEMA myschema;

DROP FUNCTION IF EXISTS myschema.TimesTwo;

CREATE FUNCTION myschema.TimesTwo(number INT)
RETURNS INTEGER
RETURN number * 2;

--
-- Query:
-- SELECT myschema.TimesTwo(10);
--
-- Output:
-- 20
--
