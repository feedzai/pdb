CREATE OR REPLACE FUNCTION GETONE ()
	RETURNS INTEGER
	NO EXTERNAL ACTION
F1: BEGIN ATOMIC
	RETURN 1;
END


CREATE OR REPLACE FUNCTION TimesTwo (VARNAME VARCHAR(128))
	RETURNS INTEGER
	NO EXTERNAL ACTION
F1: BEGIN ATOMIC
	-- ######################################################################
	-- # Returns count of all tables created by PULSE and VARNAME
	-- ######################################################################
	RETURN VARNAME * 2;
END