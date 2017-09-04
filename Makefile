.PHONY=all,licensecheck,licenseformat

MVN=mvn

all:
	$(MVN) clean install -DskipTests

licensecheck:
	$(MVN) license:check -Dlicense.header=header.txt

licenseformat:
	$(MVN) license:format -Dlicense.header=header.txt
