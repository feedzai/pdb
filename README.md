# PDB

PulseDB is a database-mapping software library in Java,
it provides a transparent access and manipulation to a great variety of database implementations.
PDB provides a DSL that covers most of SQL functionalities and allows to easily integrate persistence into your projects and modules.

## Using PDB

Add the following dependency to your Maven pom.

```
<dependencies>
    ...
	<dependency>
		<groupId>com.feedzai</groupId>
		<artifactId>pdb</artifactId>
		<version>1.0.1-SNAPSHOT</version>
	</dependency>
</dependencies>
```

## Compiling PDB

In order to compile PDB you will need to had the jar's for OracleDB, SQLServer and DB2 to your local repository.
See the following link for more information on how to do it:
- http://maven.apache.org/guides/mini/guide-3rd-party-jars-local.html

## Getting  started

### Index

- [Example Description](#example-description)
- [Establish connection](#establish-connection)
- Table Manipulation
	- [Create Table](#create-table)
	- [Drop Table](#drop-table)
	- [Alter Table](#alter-table)
- Data Manipulation
	- [Insertion Queries](#insertion-queries)
	- [Update and Delete Queries](#update-and-delete-queries)
	- [Truncate Queries](#truncate-queries)
	- [Selection Queries](#selection-queries)
- [Create View](#create-view)

### Example Description

We describe a scenario where there are some data Providers that share Streams of data with the world.
These Streams have a data Type, and they are consumed by some Modules.
The entities and its relations are modeled into SQL using PDB in the following sections.

### Establish a connection

With PDB you connect to the database of your preference using the following code.

```java
Properties properties = new Properties() {
	{
		setProperty(JDBC, "jdbc:postgresql://database/");
		setProperty(USERNAME, "username");
		setProperty(PASSWORD, "password");
		setProperty(ENGINE, "com.feedzai.commons.sql.abstraction.engine.impl.PostgreSqlEngine");
		setProperty(SCHEMA_POLICY, "create");
		setProperty(SCHEMA, "default");
	}
};

DatabaseEngine engine = DatabaseFactory.getConnection(properties);
```

Always remember to provide your own credentials and schema name.
It is also important to select a schema policy. There are four possible schema policies:
- create - New entities are created normally.
- create-drop - Same as create policy but before the connection is closed all entries created during this session will be dropped.
- drop-create - New entities are dropped before creation if they already exist.
- none - The program is not allowed to create new entities.

Note that this is an example for PostgreSql, but PDB also supports H2, MySql, Oracle, SqlServer and others.
For a more comprehensive list of the Properties, check the javadoc documentation.

### Create Table

We start by creating the table to store the different data Types:

```java
DbEntity data_type_table =
	new DbEntity()
		.setName("data_type")
			.addColumn("id", INT, UNIQUE, NOT_NULL)
			.addColumn("code", STRING, UNIQUE, NOT_NULL)
			.addColumn("description", CLOB)
			.setPkFields("id");
```

A table is represented with a DbEntity and its properties can be defined with methods:

```
entity.setName(String name)
- Select the name for this table.

entity.addColumn(String name, DbColumnType type, boolean autoincrement, DbColumnProperties... constraints)
- Create a column with a given name and type.
- Additionally you can had autoincrement behaviour and define some extra constraints.
- There are two possible constraints available: UNIQUE and NOT_NULL.

entity.setPkFields(String... columns)
- Define which columns are part of the primary key.
```

To create the data_type_table you call addEntity method on the previously created database engine.
Depending on the policy you chose existing tables may be dropped before creation.

```java
engine.addEntity(data_type_table);
```

Let's now create the Providers and Streams tables:

```java
DbEntity provider_table =
	new DbEntity()
		.setName("provider")
			.addColumn("id", INT, true, UNIQUE, NOT_NULL)
			.addColumn("uri", STRING, UNIQUE, NOT_NULL)
			.addColumn("certified", BOOLEAN, NOT_NULL)
			.addColumn("description", CLOB)
			.setPkFields("id");

engine.addEntity(provider_table);

DbEntity stream_table =
	new DbEntity()
	.setName("stream")
		.addColumn("id", INT, true, UNIQUE, NOT_NULL)
		.addColumn("provider_id", INT, NOT_NULL)
		.addColumn("data_type_id", INT, NOT_NULL)
		.addColumn("description", CLOB)
		.setPkFields("id")
		.addFk(
			new DbFk()
				.addColumn("provider_id")
				.setForeignTable("provider")
				.addForeignColumn("id"),
			new DbFk()
				.addColumn("data_type_id")
				.setForeignTable("data_type")
				.addForeignColumn("id"))
		.addIndex(false, "provider_id", "data_type_id");

engine.addEntity(stream_table);
```

You may have noticed that this stream_table has some foreign keys, which we created with addFK.
This method receives a list of the foreign keys constraints.
A foreign key is created with DbFk, and it is defined using methods:

```
.addColumn(String... columns)
- Define which columns will be part of this constraint.

.setForeignTable(String foreignTable)
- Define the foreign table we are referring to.

.addForeignColumn(String... foreignColumns)
- Selects the affected columns in the foreign table.
```

Wait! Looks like we also created an index in the Stream table.

```
.addIndex(String... columns)
.addIndex(boolean unique, String... columns)
- Creates and index for the listed columns.
- If not sepecified index is not unique.
```

The rest of the example case is created with the following code:

```java
DbEntity module_table =
	new DbEntity()
		.setName("module")
			.addColumn("name", STRING, UNIQUE, NOT_NULL);

engine.addEntity(module_table);

DbEntity stream_to_module_table =
	new DbEntity()
		.setName("stream_to_module")
			.addColumn("name", STRING, NOT_NULL)
			.addColumn("stream_id", INT, NOT_NULL)
			.addColumn("active", INT)
			.setPkFields("name", "stream_id")
			.addFk(
				new DbFk()
					.addColumn("name")
					.setForeignTable("module")
					.addForeignColumn("name"),
				new DbFk()
					.addColumn("stream_id")
					.setForeignTable("stream")
					.addForeignColumn("id"));

engine.addEntity(stream_to_module_table);
```

### Drop Table

When you are done with this example you may want to clean the database.

```java
engine.dropEntity("stream_to_module");
```
```
.dropEntity(String name)
- Drops an entity given the name.
```

### Alter Table

With PDB you can change some aspects of a previously created tables.
After calling the the addEntity method with the created entity you can continue to modify this local representation by calling the methods described in the previous sections.
Then to synchronize the local representation with the actual table in the database you call the updateEntity method.

```java
data_type_table.removeColumn("description");

engine.updateEntity(data_type_table);
```
```
.removeColumn(String name)
- Removes a column from the local representation of the table.

.updateEntity(DbEntity entity)
- Synchronizes the entity representation with the table in the database.
- If schema policy is set to drop-create the whole table is dropped and created again.
```

Another mechanism to alter table is by using the AlterColumn expression creation and the executeUpdate method provided by the database engine.
In this case changes are made to each column, one at a time.

```java
Expression alterColumn = new AlterColumn(table("stream_to_module"),
					new DbColumn("active", BOOLEAN)
						.addConstraint(NOT_NULL));

engine.executeUpdate(alterColumn);
```
```
AlterColumn(Expression table, DbColumn column)
- Creates a expression of changing a given table schema affecting a column.

DbColumn(String name, DbColumnType dbColumnType, boolean autoIncrement)
- Column definition.
- Provide new type and autoincrement behavior.

.addConstraint(DbColumnConstraint constraint)
.addConstraints(DbColumnConstraint... constraints)
- Define the constraints you want the column to oblige to.
```

It is also possible to remove the the primary key constraint.

```java
Expression dropPrimaryKey = dropPK(table("TEST"));

engine.executeUpdate(dropPrimaryKey);
```
```comment
DropPrimaryKey(Expression table)
- Drops the primary key constraint on the given table.
```

### Insertion Queries

Now that we have the structure of the database in place, let's play it with some data.
An EntityEntry it's our representation of an entry that we want to add to the database.

```java
EntityEntry data_type_entry =
	new EntityEntry()
		.set("id", 1)
		.set("code", "INT16")
		.set("description", "The type of INT you always want!");
```
```
.set(String name, Object value)
- Define the value that will be assigned to a given column.
```

Notice that the values for each column were defined using the set method.
A new entry for the database is persisted with engine's method persist.

```java
engine.persist("data_type", data_type_entry, false);
```
```
.persist(String tableName, EntityEntry entity, boolean autoincrement)
- Select the table in which the new entity will be inserted.
- If the affected table has an autoincrement column you might want to activate this flag.
- In case that the autoincrement behaviour is active, this method returns the generated key.
```

If you want to use the autoincrement behavior, you should not define the value for the affected column and activate the autoincrement flag.

```java
EntityEntry provider_entry =
	new EntityEntry()
		.set("uri", "from.some.where")
		.set("certified", true);

long generatedKey = engine.persist("provider", provider_entry, true);
```

### Batches

PDB also gives support for batches. With batches you reduce the amount of communication overhead, thereby improving performance.

```java
engine.beginTransaction();

try {
    EntityEntry entry = new EntityEntry()
    	.set("code", "SINT")
    	.set("description", "A special kind of INT");

    engine.addBatch("data_type", entry);

    entry = new EntityEntry()
    	.set("code", "VARBOOLEAN")
    	.set("description", "A boolean with a variable number of truth values");

    engine.addBatch("data_type", entry);

    engine.flush();
    engine.commit();
} finally {
    if (engine.isTransactionActive()) {
        engine.rollback();
    }
}
```
```
.beginTransaction()
- Starts a transaction.

.addBatch(EntityEntry entry)
- Adds an entry to the current batch.

.flush()
- Executes all entries registred in the batch.

.commit()
- Commits the current transaction transaction.

.isTransactionActive()
- Tests if the transaction is active.

.rollback()
- Rolls back the transaction.
```

### Update and Delete Queries

Now you may want to the update the data or simply erase it. Let's see how this is done.

```java
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

```
update()
- Creates an update query that will affect the table referred by the given expression.

.set(Expression... expressions)
- Expression that defines the values that will be assigned to each given column.

.where(Expression where)
- Expression for filtering/selecting the affected entries.

table(String table)
- Creates a reference to a table of your choice.
```

Maybe you want to delete entries instead. In that case creating a delete query is required.

```java
engine.executeUpdate(
	delete(table("stream")));

engine.executeUpdate(
	delete(table("stream"
		.where(eq(column(id), k(1))));
```
```
delete
- Creates a delete query that will affect the table referred by the given expression.

.where(Expression table)
- Expression for filtering/selecting the affected entries.
```

### Truncate Queries

If what you seek is to delete all table entries at once, it is recommended to use the truncate query.

```java
engine.executeUpdate(
	truncate(table("stream")));
```
```
truncate(Expression table)
- Creates a truncate query that will affect the table referred by the given expression.
```

### Selection Queries

Now things will get interesting. In this section we will see how PDB uses SQL to select data from the database.
Using the query method we get the result for any given query as a list of entries.
These entries are represented as a map of column name to content.

```java
Expression query =
	select(all())
	.from(table("streams"));

List<Map<String, ResultColumn>> results = engine.query(query);

for(Map<String, ResultColumn> result : results) {
    Int id = result.get("id").toInt();
    String description = result.get("description").toString();
	System.out.println(id + ": "+ description);
}
```

	.query(Query query)
	- Processes a given query and computes the corresponding result.
	- It return a List of results if any.
	- For each column a result is a Map that maps column names to ResultColumn objects.

	.toXXX()
	- ResultColumn provides methods to convert the data to the type of your preference.
	- It throws an exception if you try to convert the underlying data to some incompatible type.

Let's see this simple query in more detail.
Where we list all entries in table Streams and return all columns.

```java
results = engine.query(
	select(all())
	.from(table("streams")));
```
```
select(Expression... select)
- Expression defining the selection of columns or other manipulations of its values.

.distinct()
- Filter the query so it only returns distinct values.

.from(final Expression... sources)
- Defines what tables or combination of them the data must be fetched from.
- By default the listed sources will be joined together with an inner join.

all()
- Defines a reference to all column the underlying query might return.

k(Object obj)
- Creates a Constant from obj.

lit(Object obj)
- Creates a Literal from obj.

column(String columnName)
column(String tableName, String columnName)
- Defines a reference to a given column.
```

This is useful but not very interesting.
We should proceed by filtering the results with some condition of our choice.

```java
results = engine.query(
	select(all())
	.from(table("streams"))
	.where(eq(column("data_type_id"), k(4)))
	.andWhere(like(column("description"), k("match t%xt"))));
```
```
.where(Expression expression)
- Defines a series of testes a entry must oblige in order to be part of the result set.

.andWhere(Expression expression)
- If there is already ab where clause it defines an and expression with the old clause.

eq(Expression... expressions)
- Applies the equality condition to the expressions.
- It is also used in insertion queries to represent attribution.

neq(Expression... expressions)
- Negation of the equality condition.

like(Expression... expressions)
- Likelihood comparison between expression.
- Those expression must resolve to String constants or columns of the same type.

lt(Expression... expressions)
lteq(Expression... expressions)
gt(Expression... expressions)
gteq(Expression... expressions)
- Predicate over numerical or alphanumerical values.
```

A more complex filter would be one that select Streams from a given range of data Types and a set of Providers.
And we manage just that with the following query.

```java
results = engine.query(
	select(all())
	.from(table("streams"))
	.where(
		and(between(column("data_type_id"), k(2), k(5)),
			notIn(column("provider_id"), L(k(1), k(7), k(42))))));
```
```
and(Expression... expressions)
or(Expression... expressions)
- Computes the boolean result of the underlying expressions.

between(Expression exp1, Expression exp2, Expression exp3)
notBetween(Expression exp1, Expression exp2, Expression exp3)
- Defines a test condition that asserts if exp1 is part of the range of values from exp2 to exp3.

in(Expression exp1, Expression exp2)
notIn(Expression exp1, Expression exp2)
- Defines a test condition that asserts if exp1 is part of exp2.
- Expression exp2 might be a List of constants or the result of a sub query.

L(Expression... expressions)
- Defines a list of elements represent by the passing expressions.
```

It is widely known that greater the id greater the Stream of data.
For this purpose you just design a query that selects the maximum Stream id of data Type 4 from Provider 1.
You might just get a raise for this.

```java
results = engine.query(
	select(max(column("id")).alias("the_best"))
	.from(table("streams"))
	.where(
		and(eq(column("data_type_id"), k(4)),
			eq(column("provider_id"), k(1)))));
```
```
.alias(String alias)
- Assigns an alias to the expression.

count(Expression expression)
max(Expression expression)
min(Expression expression)
sum(Expression expression)
avg(Expression expression)
stddev(Expression expression)
- Aggregation operator for numeric values.
- They are applicable to expression involving columns.

udf(String name, Expression exp)
- If you have defined your own sql function you may access it with udf
```

Sometimes it is required to merge the content of more than one table.
For that purpose you can use joins.
They allow you to merge content of two or more table regrading some condition.
In this example we provide a little bit more flavor to the result by adding the data Type information to the Stream information.

```java
results = engine.query(
	select(all())
	from(table("stream")
		.innerJoin((table("data_type"),
					join(
					    column("stream", "data_type_id"),
					    column("data_type", "id")))));
```
```
.innerJoin(Expression table, Expression condition)
.leftOuterJoin(Expression table, Expression condition)
.rightOuterJoin(Expression table, Expression condition)
.fullOuterJoin(Expression table, Expression condition)
- Merges the table result of two expression regarding a condition.

.join(Expressions expressions)
- Applies the equality condition to the expressions.
```

The market is collapsing! The reason, some say, is that some provider messed up.
In your contract it is stated that Provider with id 4 provides a given number of streams for each data_type.
With the following query you will find out if the actual data in the database matches the contract.
By filtering the results to only account for Provider 4 and grouping on the data Type you are able to count the number of streams by Type.
You Boss will be pleased.

```java
results = engine.query(
	select(column("data_type_id"), count(column("id")).alias("count"))
	.from(table("streams"))
	.where(eq(column("provider_id"), k(4)))
	.groupby(column("data_type_id"))
	.orderby(column("data_type_id")).asc());
```
```
.groupby(Expression... groupbyColumns)
- Groups the result on some of the table columns.

.orderby(Expression... orderbyColumns)
- Orders the result according to some expression of the table columns.

.asc()
.desc()
- Sets the ordering of your choice, either ascendant or descendant.
```

Some documents leaked online last week suggest that there are some hidden message in our data.
To visualize this hidden message we need to do some arithmetic's with the ids of the provider and data_type on table Streams.
Even more strange is the need to filter the description column, where in case of a null value an alternative is presented.

```java
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
```
minus(Expression... expressions)
mult(Expression... expressions)
plus(Expression... expressions)
div(Expression... expressions)
mod(Expression... expressions)
- Operators to manipulate numeric values.
- Applies the respective operator to the list of value with left precedence.

coalesce(Expression expression, Expression... alternatives)
- Coalesce tests a given expression and returns its value if it is not null.
- If the primary expression is null, it will return the first alternative that is not.
```

For this next example, imagine you want to select all Streams for which the sum of data_type_id and provider_id is greater than 5.
It might not be a very useful query, but when you had that you just want 10 rows of the result with and offset of 2, people might wonder what you are up to.

```java
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
```
having(Expression having)
- Query will select only the result rows where aggregate values meet the specified conditions.

limit(Integer limit)
- Defines the number of rows that the query returns.

offset(Integer offset)
- Defines the offset for the start position of the resulting rows.
```

### Create View

Sometimes, for security reasons or just for simplicity, it is useful to have a view of the database.

```java
Expression view = createView("simple_stream")
					.as(select(column("id"), column("data_type_id"))
						.from(table("stream")))
					.replace();

engine.executeUpdate(view);
```
```
createView(String name)
- Creates a view with the given name.

.as(Query query)
- Defines the query that provides the data for this view.

.replace()
- Whether or not the view creation is authorized to overwrite over existing views.
```

## Further Documentation

For more insight on the available functionality please consult the javadoc documentation.

## Contact

For more information please send us a message to opensource@feedzai.com, we will happily answer your questions.

## License

See [LICENSE](https://github.com/feedzai/pdb/blob/master/LICENSE).
