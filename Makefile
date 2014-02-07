.PHONY=all,test-mysql,test-db2,test-oracle,test-postgres,test-postgresql,test-sqlserver,test-all

MVN=mvn

all:
	mvn clean install -DskipTests

licensecheck:
	$(MVN) license:check -Dlicense.header=header.txt

licenseformat:
	$(MVN) license:format -Dlicense.header=header.txt

test-mysql:
	$(MVN) test -Pmysql55
	$(MVN) test -Pmysql56

test-db2:
	$(MVN) test -Pdb2102
	$(MVN) test -Pdb2105

test-oracle:
	$(MVN) test -Poracle11
	$(MVN) test -Poracle12

test-postgres:
	$(MVN) test -Ppostgresql849
	$(MVN) test -Ppostgresql931

test-postgresql: test-postgres

test-sqlserver:
	$(MVN) test -Psqlserver2008
	$(MVN) test -Psqlserver2012

test-all: test-mysql test-db2 test-oracle test-postgres test-sqlserver
