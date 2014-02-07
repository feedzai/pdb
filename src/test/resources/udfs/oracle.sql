--
-- the following statements must be run separetely
--

CREATE OR REPLACE FUNCTION GetOne
RETURN INTEGER
AS
BEGIN
  RETURN 1;
END GetOne;

--
-- Query:
-- SELECT GetOne() FROM dual;
--
-- Output:
-- 1
--

CREATE OR REPLACE FUNCTION TimesTwo (n IN INTEGER)
RETURN INTEGER
AS
BEGIN
  RETURN n * 2;
END TimesTwo;

--
-- Query:
-- SELECT TimesTwo(10) FROM dual;
--
-- Output:
-- 20
--
