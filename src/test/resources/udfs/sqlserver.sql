USE [pdb]
GO

IF OBJECT_ID (N'GetOne', N'FN') IS NOT NULL
    DROP FUNCTION GetOne;
GO

CREATE FUNCTION GetOne()
RETURNS INTEGER
AS
BEGIN
  RETURN(1)
END
GO

--
-- Query (sqlserver always requires a schema when calling functions):
-- SELECT dbo.GetOne();
--
-- Output:
-- 1
--

IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'myschema')
BEGIN
	IF OBJECT_ID (N'myschema.TimesTwo', N'FN') IS NOT NULL
	BEGIN
		DROP FUNCTION myschema.TimesTwo;
	END
	DROP SCHEMA myschema;
END
GO

CREATE SCHEMA myschema;
GO

CREATE FUNCTION myschema.TimesTwo(@number INTEGER)
RETURNS INTEGER
AS
BEGIN
  RETURN(@number * 2)
END
GO

--
-- Query (sqlserver always requires a schema when calling functions):
-- SELECT myschema.TimesTwo(10);
--
-- Output:
-- 20
--
