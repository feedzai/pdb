# PDB

PulseDB is a database-mapping software library written in Java,
it provides a transparent access and manipulation to a great variety of database implementations.
PDB provides a DSL that covers most of SQL functionalities and allows to easily integrate persistence into your projects and modules.

[![Build Status](https://feedzaios.ci.cloudbees.com/buildStatus/icon?job=pdb-master-java-8)](https://feedzaios.ci.cloudbees.com/job/pdb-master-java-8/)

[![CloudbeesDevCloud](http://www.cloudbees.com/sites/default/files/Button-Built-on-CB-1.png)](http://www.cloudbees.com/dev)

## Using PDB

Add the following dependency to your Maven pom.

```
<dependencies>
    ...
	<dependency>
		<groupId>com.feedzai</groupId>
		<artifactId>pdb</artifactId>
		<version>2.0.1</version>
	</dependency>
	...
</dependencies>
```

## Changes from 2.0.0
* It is now possible to call built-in database vendor functions [e.g. f("lower", column("COL1"))]
* Added lower and upper functions
* Fixed several connection leaks
* Fixed MySQL large result fetching

## Compiling PDB

In order to compile PDB you will need to have the Oracle Driver JAR in your local repository.
The current version assumes Oracle Driver version 11.2.0.2.0. Please download the driver [here] and
run the following to install the driver in your local maven repository.
[here]:http://www.oracle.com/technetwork/database/enterprise-edition/jdbc-112010-090769.html
```bash
mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc6 \
-Dversion=11.2.0.2.0 -Dpackaging=jar -Dfile=ojdbc6-11.2.0.2.0.jar
```


## Getting  started

### Index

- [Example Description](#example-description)
- [Establishing connection](#establishing-connection)
- Table Manipulation
	- [Create Table](#create-table)
	- [Drop Table](#drop-table)
	- [Alter Table](#alter-table)
- Data Manipulation
	- [Insertion Queries](#insertion-queries)
	- [Batches](#batches)
	- [Updating and Deleting Queries](#updating-and-deleting-queries)
	- [Truncate Queries](#truncate-queries)
	- [Selection Queries](#selection-queries)
	- [Prepared Statements](#prepared-statements)
- [Create View](#create-view)

### Example Description

We describe a scenario where there are some data Providers that share Streams of data with the world.
These Streams have a data Type, and they are consumed by some Modules.
The entities and its relations are modeled into SQL using PDB in the following sections.

### Establishing a connection

With PDB you connect to the database of your preference using the following code.

```java
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;

(...)

Properties properties = new Properties() {
	{
		setProperty(JDBC, "<JDBC-CONNECTION-STRING>");
		setProperty(USERNAME, "username");
		setProperty(PASSWORD, "password");
		setProperty(ENGINE, "<PDB-ENGINE>");
		setProperty(SCHEMA_POLICY, "create");
	}
};

DatabaseEngine engine = DatabaseFactory.getConnection(properties);
```
The following table shows how to connect for the supported database vendors.

|Vendor|Engine|JDBC|
|:---|:---|:---|
|DB2|com.feedzai.commons.sql.abstraction.engine.impl.DB2Engine|jdbc:db2://&lt;HOST&gt;:&lt;PORT&gt;/&lt;DATABASE&gt;|
|Oracle|com.feedzai.commons.sql.abstraction.engine.impl.OracleEngine|jdbc:oracle:thin:@&lt;HOST&gt;:1521:&lt;DATABASE&gt;|
|PostgreSQL|com.feedzai.commons.sql.abstraction.engine.impl.PostgreSqlEngine|jdbc:postgresql://&lt;HOST&gt;/&lt;DATABASE&gt;|
|MySQL|com.feedzai.commons.sql.abstraction.engine.impl.MySqlEngine|jdbc:mysql://&lt;HOST&gt;/&lt;DATABASE&gt;|
|H2|com.feedzai.commons.sql.abstraction.engine.impl.H2Engine|jdbc:h2:&lt;FILE&gt; &#124; jdbc:h2:mem|
|SQLServer|com.feedzai.commons.sql.abstraction.engine.impl.SqlServerEngine|jdbc:sqlserver://&lt;HOST&gt;;database=&lt;DATABASE&gt;|


It is also important to select a schema policy. There are four possible schema policies:
- create - New entities are created normally.
- create-drop - Same as create policy but before the connection is closed all entries created during this session will be dropped.
- drop-create - New entities are dropped before creation if they already exist.
- none - The program is not allowed to create new entities.

### Create Table

We start by creating the table to store the different data Types:

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

DbEntity data_type_table =
	dbEntity()
		.name("data_type")
		.addColumn("id", INT, UNIQUE, NOT_NULL)
		.addColumn("code", STRING, UNIQUE, NOT_NULL)
		.addColumn("description", CLOB)
		.pkFields("id")
		.build();
```

A table is represented with a DbEntity and its properties can be defined with methods:

|Function|Description|
|:---|:---|
|[name]|Select the name for this table.|
|[addColumn]|Create a column with a given name and type. Additionally you can had autoincrement behaviour and define some extra constraints. There are two possible constraints available: UNIQUE and NOT_NULL.|
|[pkFields]|Define which columns are part of the primary key.|
[name]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/ddl/DbEntity.Builder.html#name(java.lang.String)
[addColumn]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/ddl/DbEntity.Builder.html#pkFields(java.util.Collection)
[pkFields]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/ddl/DbEntity.Builder.html#pkFields(java.util.Collection)

To create the data_type_table you call addEntity method on the previously created database engine.
Depending on the policy you chose existing tables might be dropped before creation.

```java
engine.addEntity(data_type_table);
```

Let's now create the Providers and Streams tables:

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

DbEntity provider_table =
	dbEntity()
		.name("provider")
			.addColumn("id", INT, true, UNIQUE, NOT_NULL)
			.addColumn("uri", STRING, UNIQUE, NOT_NULL)
			.addColumn("certified", BOOLEAN, NOT_NULL)
			.addColumn("description", CLOB)
			.pkFields("id")
			.build();

engine.addEntity(provider_table);

DbEntity stream_table =
	dbEntity()
	.name("stream")
		.addColumn("id", INT, true, UNIQUE, NOT_NULL)
		.addColumn("provider_id", INT, NOT_NULL)
		.addColumn("data_type_id", INT, NOT_NULL)
		.addColumn("description", CLOB)
		.pkFields("id")
		.addFk(
			dbFk()
				.addColumn("provider_id")
				.foreignTable("provider")
				.addForeignColumn("id"),
			dbFk()
				.addColumn("data_type_id")
				.foreignTable("data_type")
				.addForeignColumn("id"))
		.addIndex(false, "provider_id", "data_type_id")
		.build();

engine.addEntity(stream_table);
```

You may have noticed that this stream_table has some foreign keys, which we define using the addFK method.
This method receives a list of the foreign keys constraints.
A foreign key is created with dbFk(), and it is defined using these methods:

|Function|Description|
|:---|:---|
|[addColumn]|Define which columns will be part of this constraint.|
|[foreignTable]|Define the foreign table we are referring to.|
|[addForeignColumn]|Selects the affected columns in the foreign table.|
[addColumn]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/ddl/DbFk.Builder.html#addColumn(java.lang.String...)
[foreignTable]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/ddl/DbFk.Builder.html#foreignTable(java.lang.String)
[addForeignColumn]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/ddl/DbFk.Builder.html#addColumn(java.lang.String...)

Wait! Looks like we also created an index in the Stream table.

|Function|Description|
|:---|:---|
|[addIndex]|Creates and index for the listed columns. If not specified, an index is not unique.|
[addIndex]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/ddl/DbEntity.Builder.html#addIndex(boolean,%20java.lang.String...)

The rest of the example case is created with the following code:

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

DbEntity module_table =
	dbEntity()
		.name("module")
        .addColumn("name", STRING, UNIQUE, NOT_NULL)
        .build();

engine.addEntity(module_table);

DbEntity stream_to_module_table =
	dbEntity()
		.name("stream_to_module")
			.addColumn("name", STRING, NOT_NULL)
			.addColumn("stream_id", INT, NOT_NULL)
			.addColumn("active", INT)
			.pkFields("name", "stream_id")
			.addFk(
				dbFk()
					.addColumn("name")
					.foreignTable("module")
					.addForeignColumn("name"),
				dbFk()
					.addColumn("stream_id")
					.foreignTable("stream")
					.addForeignColumn("id"))
            .build();

engine.addEntity(stream_to_module_table);
```

### Drop Table

When you are done with this example you might want to clean the database.

```java
engine.dropEntity("stream_to_module");
```
|Function|Description|
|:---|:---|
|[dropEntity]|Drops an entity given the name.|
[dropEntity]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#dropEntity(java.lang.String)

### Alter Table

With PDB you can change some aspects of a previously created tables.
After calling the the addEntity method with the created entity you can continue to modify this local representation by calling the methods described in the previous sections.
Then to synchronize the local representation with the actual table in the database you call the updateEntity method.

```java
data_type_table = data_type_table
                    .newBuilder()
                    .removeColumn("description")
                    .build();

engine.updateEntity(data_type_table);
```
|Function|Description|
|:---|:---|
|[removeColumn]|Removes a column from the local representation of the table.|
|[updateEntity]|Synchronizes the entity representation with the table in the database. If schema policy is set to drop-create the whole table is dropped and created again.|
[removeColumn]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/ddl/DbEntity.Builder.html#removeColumn(java.lang.String)
[updateEntity]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#updateEntity(com.feedzai.commons.sql.abstraction.ddl.DbEntity)

Another mechanism to alter table is by using the AlterColumn expression creation and the executeUpdate method provided by the database engine.
In this case changes are made to each column, one at a time.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

Expression alterColumn = alterColumn(
                            table("stream_to_module"),
					        dbColumn("active", BOOLEAN).addConstraint(NOT_NULL).build());

engine.executeUpdate(alterColumn);
```
|Function|Description|
|:---|:---|
|[alterColumn]|Creates a expression of changing a given table schema affecting a column.|
|[dbColumn]|Column definition. Provide new type and autoincrement behavior.|
|[addConstraint]|Define the constraints you want the column to oblige to.|
|[addConstraints]|Define the constraints you want the column to oblige to.|
[alterColumn]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#alterColumn(com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.ddl.DbColumn)
[dbColumn]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#dbColumn(java.lang.String,%20com.feedzai.commons.sql.abstraction.ddl.DbColumnType)
[addConstraint]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/ddl/DbColumn.Builder.html#addConstraint(com.feedzai.commons.sql.abstraction.ddl.DbColumnConstraint)
[addConstraints]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/ddl/DbColumn.Builder.html#addConstraints(java.util.Collection)

It is also possible to remove the the primary key constraint.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

Expression dropPrimaryKey = dropPK(table("TEST"));

engine.executeUpdate(dropPrimaryKey);
```
|Function|Description|
|:---|:---|
|[dropPK]|Drops the primary key constraint on the given table.|
[dropPK]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#dropPK(com.feedzai.commons.sql.abstraction.dml.Expression)

### Insertion Queries

Now that we have the structure of the database in place, let's play it with some data.
An EntityEntry it's our representation of an entry that we want to add to the database.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

EntityEntry data_type_entry =
	entry()
		.set("id", 1)
		.set("code", "INT16")
		.set("description", "The type of INT you always want!")
		.build();
```
|Function|Description|
|:---|:---|
|[set]|Define the value that will be assigned to a given column.|
[set]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/entry/EntityEntry.Builder.html#set(java.lang.String,%20java.lang.Object)

Notice that the values for each column were defined using the set method.
A new entry for the database is persisted with engine's method persist.

```java
engine.persist("data_type", data_type_entry, false);
```
|Function|Description|
|:---|:---|
|[persist]|Select the table in which the new entity will be inserted. If the affected table has an autoincrement column you might want to activate this flag. In case that the autoincrement behaviour is active, this method returns the generated key.|
[persist]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#persist(java.lang.String,%20com.feedzai.commons.sql.abstraction.entry.EntityEntry)

If you want to use the autoincrement behavior you must activate the autoincrement flag when defining the entity.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

EntityEntry provider_entry =
	entry()
		.set("uri", "from.some.where")
		.set("certified", true)
		.build();

long generatedKey = engine.persist("provider", provider_entry, true);
```

### Batches

PDB also provides support for batches. With batches you reduce the amount of communication overhead, thereby improving performance.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

engine.beginTransaction();

try {
    EntityEntry entry = entry()
    	.set("code", "SINT")
    	.set("description", "A special kind of INT")
    	.build();

    engine.addBatch("data_type", entry);

    entry = entry()
    	.set("code", "VARBOOLEAN")
    	.set("description", "A boolean with a variable number of truth values")
    	.build();

    engine.addBatch("data_type", entry);

    // Perform more additions...

    engine.flush();
    engine.commit();
} finally {
    if (engine.isTransactionActive()) {
        engine.rollback();
    }
}
```
|Function|Description|
|:---|:---|
|[beginTransaction]|Starts a transaction.|
|[addBatch]|Adds an entry to the current batch.|
|[flush]|Executes all entries registred in the batch.|
|[commit]|Commits the current transaction transaction.|
|[isTransactionActive]|Tests if the transaction is active.|
|[rollback]|Rolls back the transaction.|
[beginTransaction]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#beginTransaction()
[addBatch]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#addBatch(java.lang.String,%20com.feedzai.commons.sql.abstraction.entry.EntityEntry)
[flush]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#flush()
[commit]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#commit()
[isTransactionActive]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#isTransactionActive()
[rollback]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#rollback()

### Updating and Deleting Queries

Now you might want to the update data or simply erase them. Let's see how this can be done.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

engine.executeUpdate(
	update(table("stream"))
	.set(
		eq(column("cd"), k("Double")),
		eq(column("description", k("Double precision floating point number")))
	.where(eq(column(id), k(1))));
```

Expressions that produce changes to the database are executed with engine's executeUpdate method.
There are some defined static methods that allow you to create SQL queries.
Update is one of them.
In this section we describe queries that make changes to the database, while in the following section selection queries will be present in detail.

|Function|Description|
|:---|:---|
|[update]|Creates an update query that will affect the table referred by the given expression.|
|[set]|Expression that defines the values that will be assigned to each given column.|
|[where]|Expression for filtering/selecting the affected entries.|
|[table]|Creates a reference to a table of your choice.|
[update]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#update(com.feedzai.commons.sql.abstraction.dml.Expression)
[set]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Update.html#set(com.feedzai.commons.sql.abstraction.dml.Expression...)
[where]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Update.html#where(com.feedzai.commons.sql.abstraction.dml.Expression)
[table]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#table(java.lang.String)

Maybe you want to delete entries instead. In that case creating a delete query is required.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

engine.executeUpdate(
	delete(table("stream")));

engine.executeUpdate(
	delete(table("stream"
		.where(eq(column(id), k(1))));
```

|Function|Description|
|:---|:---|
|[delete]|Creates a delete query that will affect the table referred by the given expression.|
|[where]|Expression for filtering/selecting the affected entries.|
[delete]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#delete(com.feedzai.commons.sql.abstraction.dml.Expression)
[where]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Delete.html#where(com.feedzai.commons.sql.abstraction.dml.Expression)

### Truncate Queries

If what you seek is to delete all table entries at once, it is recommended to use the truncate query.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

engine.executeUpdate(truncate(table("stream")));
```

|Function|Description|
|:---|:---|
|[truncate]|Creates a truncate query that will affect the table referred by the given expression.|
[truncate]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#truncate(com.feedzai.commons.sql.abstraction.dml.Expression)

### Selection Queries

Now things will get interesting. In this section we will see how PDB uses SQL to select data from the database.
Using the query method we get the result for any given query as a list of entries.
These entries are represented as a map of column name to content.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

Expression query =
	select(all())
	.from(table("streams"));

// Fetches everything! Use with care when you know the result set is small.
List<Map<String, ResultColumn>> results = engine.query(query);

for(Map<String, ResultColumn> result : results) {
    Int id = result.get("id").toInt();
    String description = result.get("description").toString();
	System.out.println(id + ": "+ description);
}

// If your result set is large consider using the iterator.
ResultIterator it = engine.iterator(select(all()).from(table("streams")));
Map<String, ResultColumn> next;
while ((next = it.next()) != null) {
    Int id = next.get("id").toInt();
    String description = next.get("description").toString();
	System.out.println(id + ": "+ description);
}
```
The iterator closes automatically when it reaches the end of the result set, but you can close it on any time by calling it.close().

|Function|Description|
|:---|:---|
|[query]|Processes a given query and computes the corresponding result. It returns a List of results if any. For each column a result is a Map that maps
column names to ResultColumn objects.|
|[iterator]|Returns an iterator to cycle through the result set. Preferable when dealing with large result sets.|
|[toXXX]|ResultColumn provides methods to convert the data to the type of your preference. It throws an exception if you try to convert the underlying data to some incompatible type.|
[query]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#query(com.feedzai.commons.sql.abstraction.dml.Expression)
[iterator]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#iterator(com.feedzai.commons.sql.abstraction.dml.Expression)
[toXXX]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/result/ResultColumn.html

Let's see this simple query in more detail.
Where we list all entries in table Streams and return all columns.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

results = engine.query(
	select(all())
	.from(table("streams")));
```
|Function|Description|
|:---|:---|
|[select]|Expression defining the selection of columns or other manipulations of its values.|
|[distinct]|Filter the query so it only returns distinct values.|
|[from]|Defines what tables or combination of them the data must be fetched from. By default the listed sources will be joined together with an inner join.|
|[all]|Defines a reference to all column the underlying query might return.|
|[k]|Creates a Constant from obj.|
|[lit]|Creates a Literal from obj.|
|[column]|Defines a reference to a given column.|
[select]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#select(com.feedzai.commons.sql.abstraction.dml.Expression...)
[distinct]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Query.html#distinct()
[from]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Query.html#from(com.feedzai.commons.sql.abstraction.dml.Expression...)
[all]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#all()
[k]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#k(java.lang.Object)
[lit]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#lit(java.lang.Object)
[column]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#column(java.lang.String)

This is useful but not very interesting.
We should proceed by filtering the results with some condition of our choice.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

results = engine.query(
	select(all())
	.from(table("streams"))
	.where(eq(column("data_type_id"), k(4)))
	.andWhere(like(column("description"), k("match t%xt"))));
```
|Function|Description|
|:---|:---|
|[where]|Defines a series of testes a entry must oblige in order to be part of the result set.|
|[andWhere]|If there is already ab where clause it defines an and expression with the old clause.|
|[eq]|Applies the equality condition to the expressions. It is also used in insertion queries to represent attribution.|
|[neq]|Negation of the equality condition.|
|[like]|Likelihood comparison between expression. Those expression must resolve to String constants or columns of the same type.|
|[lt]|Predicate over numerical or alphanumerical values.|
|[lteq]|Predicate over numerical or alphanumerical values.|
|[gt]|Predicate over numerical or alphanumerical values.|
|[gteq]|Predicate over numerical or alphanumerical values.|
[where]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Query.html#where(com.feedzai.commons.sql.abstraction.dml.Expression)
[andWhere]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Query.html#andWhere(com.feedzai.commons.sql.abstraction.dml.Expression)
[eq]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#eq(com.feedzai.commons.sql.abstraction.dml.Expression...)
[neq]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#neq(com.feedzai.commons.sql.abstraction.dml.Expression...)
[like]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#like(com.feedzai.commons.sql.abstraction.dml.Expression...)
[lt]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#lt(com.feedzai.commons.sql.abstraction.dml.Expression...)
[lteq]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#lteq(com.feedzai.commons.sql.abstraction.dml.Expression...)
[gt]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#gt(com.feedzai.commons.sql.abstraction.dml.Expression...)
[gteq]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#gteq(com.feedzai.commons.sql.abstraction.dml.Expression...)

A more complex filter would be one that select Streams from a given range of data Types and a set of Providers.
And we manage just that with the following query.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

results = engine.query(
	select(all())
	.from(table("streams"))
	.where(
		and(between(column("data_type_id"), k(2), k(5)),
			notIn(column("provider_id"), L(k(1), k(7), k(42))))));
```
|Function|Description|
|:---|:---|
|[and]|Computes the boolean result of the underlying expressions.|
|[or]|Computes the boolean result of the underlying expressions.|
|[between]|Defines a test condition that asserts if exp1 is part of the range of values from exp2 to exp3.|
|[notBetween]|Defines a test condition that asserts if exp1 is part of the range of values from exp2 to exp3.|
|[in]|Defines a test condition that asserts if exp1 is part of exp2. Expression exp2 might be a List of constants or the result of a sub query.|
|[notIn]|Defines a test condition that asserts if exp1 is part of exp2. Expression exp2 might be a List of constants or the result of a sub query.|
|[L]|Defines a list of elements represent by the passing expressions.|
[and]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#and(com.feedzai.commons.sql.abstraction.dml.Expression...)
[or]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#or(com.feedzai.commons.sql.abstraction.dml.Expression...)
[between]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#between(com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression)
[notBetween]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#notBetween(com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression)
[in]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#in(com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression)
[notIn]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#notIn(com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression)
[L]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#L(com.feedzai.commons.sql.abstraction.dml.Expression...)

It is widely known that greater the id greater the Stream of data.
For this purpose you just design a query that selects the maximum Stream id of data Type 4 from Provider 1.
You might just get a raise for this.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

results = engine.query(
	select(max(column("id")).alias("the_best"))
	.from(table("streams"))
	.where(
		and(eq(column("data_type_id"), k(4)),
			eq(column("provider_id"), k(1)))));
```
|Function|Description|
|:---|:---|
|[alias]|Assigns an alias to the expression.|
|[count]|Aggregation operator for numeric values. They are applicable to expression involving columns.|
|[max]|Aggregation operator for numeric values. They are applicable to expression involving columns.|
|[min]|Aggregation operator for numeric values. They are applicable to expression involving columns.|
|[sum]|Aggregation operator for numeric values. They are applicable to expression involving columns.|
|[avg]|Aggregation operator for numeric values. They are applicable to expression involving columns.|
|[stddev]|Aggregation operator for numeric values. They are applicable to expression involving columns.|
|[udf]|If you have defined your own sql function you may access it with udf.|
[alias]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Expression.html#alias(java.lang.String)
[count]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#count(com.feedzai.commons.sql.abstraction.dml.Expression)
[max]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#min(com.feedzai.commons.sql.abstraction.dml.Expression)
[min]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#max(com.feedzai.commons.sql.abstraction.dml.Expression)
[sum]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#sum(com.feedzai.commons.sql.abstraction.dml.Expression)
[avg]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#avg(com.feedzai.commons.sql.abstraction.dml.Expression)
[stddev]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#stddev(com.feedzai.commons.sql.abstraction.dml.Expression)
[udf]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#udf(java.lang.String,%20com.feedzai.commons.sql.abstraction.dml.Expression)

Sometimes it is required to merge the content of more than one table.
For that purpose you can use joins.
They allow you to merge content of two or more table regrading some condition.
In this example we provide a little bit more flavor to the result by adding the data Type information to the Stream information.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

results = engine.query(
	select(all())
	from(table("stream")
		.innerJoin((table("data_type"),
					join(
					    column("stream", "data_type_id"),
					    column("data_type", "id")))));
```
|Function|Description|
|:---|:---|
|[innerJoin]|Merges the table results of two expression regarding a condition.|
|[leftOuterJoin]|Merges the table results of two expression regarding a condition.|
|[rightOuterJoin]|Merges the table results of two expression regarding a condition.|
|[fullOuterJoin]|Merges the table results of two expression regarding a condition.|
|[join]|Applies the equality condition to the expressions.|
[innerJoin]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Expression.html#innerJoin(com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression)
[leftOuterJoin]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Expression.html#leftOuterJoin(com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression)
[rightOuterJoin]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Expression.html#rightOuterJoin(com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression)
[fullOuterJoin]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Expression.html#fullOuterJoin(com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression)
[join]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#join(com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression)

The market is collapsing! The reason, some say, is that some provider messed up.
In your contract it is stated that Provider with id 4 provides a given number of streams for each data_type.
With the following query you will find out if the actual data in the database matches the contract.
By filtering the results to only account for Provider 4 and grouping on the data Type you are able to count the number of streams by Type.
Your Boss will be pleased.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

results = engine.query(
	select(column("data_type_id"), count(column("id")).alias("count"))
	.from(table("streams"))
	.where(eq(column("provider_id"), k(4)))
	.groupby(column("data_type_id"))
	.orderby(column("data_type_id").asc());
```
|Function|Description|
|:---|:---|
|[groupby]|Groups the result on some of the table columns.|
|[orderby]|Orders the result according to some expression of the table columns.|
|[asc]|Sets the ordering as ascendant.|
|[desc]|Sets the ordering as descendant.|
[groupby]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Query.html#groupby(com.feedzai.commons.sql.abstraction.dml.Expression...)
[orderby]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Query.html#orderby(com.feedzai.commons.sql.abstraction.dml.Expression...)
[asc]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Expression.html#asc()
[desc]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Expression.html#desc()

Some documents leaked online last week suggest that there are some hidden message in our data.
To visualize this hidden message we need to do some arithmetic's with the ids of the provider and data_type on table Streams.
Even more strange is the need to filter the description column, where in case of a null value an alternative is presented.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

results = engine.query(
	select(
		plus(
			column("data_type_id"),
			column("provider_id"),
			k(1)
		).alias("mess_ids"),
		coalesce(
			column("description"),
			k("Romeo must die")))
	.from(table("streams")));
```
|Function|Description|
|:---|:---|
|[minus]|Applies the subtraction operator to the list of value with left precedence.|
|[mult]|Applies the multiplication operator to the list of value with left precedence.|
|[plus]|Applies the addiction operator to the list of value with left precedence.|
|[div]|Applies the division operator to the list of value with left precedence.|
|[mod]|Applies the module operator to the list of value with left precedence.|
|[coalesce]|Coalesce tests a given expression and returns its value if it is not null. If the primary expression is null, it will return the first alternative that is not.|
[minus]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#minus(com.feedzai.commons.sql.abstraction.dml.Expression...)
[mult]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#mult(com.feedzai.commons.sql.abstraction.dml.Expression...)
[plus]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#plus(com.feedzai.commons.sql.abstraction.dml.Expression...)
[div]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#div(com.feedzai.commons.sql.abstraction.dml.Expression...)
[mod]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#mod(com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression)
[coalesce]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#coalesce(com.feedzai.commons.sql.abstraction.dml.Expression,%20com.feedzai.commons.sql.abstraction.dml.Expression...)

For this next example, imagine you want to select all Streams for which the sum of data_type_id and provider_id is greater than 5.
It might not be a very useful query, but when you had that you just want 10 rows of the result with and offset of 2, people might wonder what you are up to.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

results = engine.query(
	select(all())
	.from(table("streams"))
	.having(
		gt(plus(
				column("data_type_id"),
				column("provider_id")),
			k(5)))
	.limit(10)
	.offset(2));
```
|Function|Description|
|:---|:---|
|[having]|Query will select only the result rows where aggregate values meet the specified conditions.|
|[limit]|Defines the number of rows that the query returns.|
|[offset]|Defines the offset for the start position of the resulting rows.|
[having]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Query.html#having(com.feedzai.commons.sql.abstraction.dml.Expression)
[limit]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Query.html#limit(java.lang.Integer)
[offset]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/Query.html#offset(java.lang.Integer)

### Prepared Statements

PDB also allows the creation of prepared statements.
Here you have two of the previous example queries done using prepared statements

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

Query query = select(all())
		.from(table("streams"))
		.where(eq(column("data_type_id"), lit("?")))
		.andWhere(like(column("description"), k("match t%xt")));

engine.createPreparedStatement("MyPS", query);

// It is always a good policy to clear the parameters
engine.clearParameters("MyPS");
engine.setParameter("MyPS", 1, 10);

engine.executePS("MyPS");
List<Map<String, ResultColumn>> result = engine.getPSResultSet("MyPS");
```
In PDB prepared statements are stored internally and they are maintained if the connection is lost, but the parameters are always lost.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

Update update = update(table("stream"))
			.set(
				eq(column("cd"), lit("?")),
				eq(column("description", lit("?")))
			.where(eq(column(id), k(1))));

engine.createPreparedStatement("MyPS", query);

engine.clearParameters("MyPS");
engine.setParameter("MyPS", 1, "INT");
engine.setParameter("MyPS", 2, "Your regular integer implementation.");

int affectedEntries = engine.executePSUpdate("MyPS");
```

|Function|Description|
|:---|:---|
|[createPreparedStatement]|Creates a prepared statement and assigns it to a given identifier.|
|[clearParameters]|Clears the parameters of a given prepared statement.|
|[setParameter]|Assigns a object to a given parameter of a prepared statement.|
|[executePS]|Executes a given prepared statement.|
|[executePSUpdate]|Executes a given update prepared statement and returns the number of affected rows.|
|[getPSResultSet]|Returns the result set of the last executed query.|
|[getPSIterator]|Returns an iterator to the result set of the last executed query.|
[createPreparedStatement]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#createPreparedStatement(java.lang.String,%20com.feedzai.commons.sql.abstraction.dml.Expression)
[clearParameters]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#clearParameters(java.lang.String)
[setParameter]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#setParameter(java.lang.String,%20int,%20java.lang.Object)
[executePS]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#executePS(java.lang.String)
[executePSUpdate]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#executePSUpdate(java.lang.String)
[getPSResultSet]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#getPSResultSet(java.lang.String)
[getPSIterator]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/engine/AbstractDatabaseEngine.html#getPSIterator(java.lang.String)

### Create View

Sometimes, for security reasons or just for simplicity, it is useful to have a view of the database.

```java
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

(...)

Expression view = createView("simple_stream")
					.as(select(column("id"), column("data_type_id"))
						.from(table("stream")))
					.replace();

engine.executeUpdate(view);
```
|Function|Description|
|:---|:---|
|[createView]|Creates a view with the given name.|
|[as]|Defines the query that provides the data for this view.|
|[replace]|Whether or not the view creation is authorized to overwrite over existing views.|
[createView]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/dialect/SqlBuilder.html#createView(java.lang.String)
[as]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/View.html#as(com.feedzai.commons.sql.abstraction.dml.Expression)
[replace]:http://feedzai.github.io/pdb/com/feedzai/commons/sql/abstraction/dml/View.html#replace()

## Further Documentation

For more insight on the available functionality please see projects [javadoc](http://feedzai.github.io/pdb).

## Contact

For more information please contact opensource@feedzai.com, we will happily answer your questions.

## Special Thanks
* Miguel Miranda (miguelnmiranda@gmail.com) for the documentation and first steps on making this library opensource

## License

Copyright 2014 Feedzai

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
