/*
 * Copyright 2021 Feedzai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.feedzai.commons.sql.abstraction.engine.impl.abs;

import com.feedzai.commons.sql.abstraction.ddl.DbColumnConstraint;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.ddl.DbFk;
import com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.google.common.collect.ImmutableList;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.STRING;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.all;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.column;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbFk;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.delete;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.entry;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.eq;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.k;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.select;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for foreign keys defined in the entities.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 * @since 2.8.1
 */
@RunWith(Parameterized.class)
public class ForeignKeyTest {

    private DatabaseEngine engine;
    private Properties properties;
    private DbEntity userEntity;

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    /**
     * Initializes the database engine for the tests and creates a "USER" entity with 1 row, to serve as the primary table
     * to be referenced in foreign keys from other tables.
     *
     * @throws Exception If there is a problem performing the test preparations.
     */
    @Before
    public void init() throws Exception {
        properties = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "drop-create");
            }
        };

        engine = DatabaseFactory.getConnection(properties);

        /*
         The tests will create foreign keys that refer to 1 or 2 columns in this reference table:
         - the columns that define the primary key can be used as referenced columns in foreign keys that have 2 columns
         - the column with "unique" constraint can be used as referenced column in foreign keys that have 1 column;
           this also needs to be "not null" because DB2 doesn't support nulls in unique columns
         */
        userEntity = dbEntity()
                .name("USER")
                .addColumn("USER_ID", INT, DbColumnConstraint.UNIQUE, DbColumnConstraint.NOT_NULL)
                .addColumn("AUTH_TYPE", STRING)
                .pkFields("USER_ID", "AUTH_TYPE")
                .build();

        engine.addEntity(userEntity);
        engine.persist("USER", entry().set("USER_ID", 1).set("AUTH_TYPE", "LOCAL").build());
    }

    @After
    public void cleanup() {
        engine.close();
    }

    /**
     * Tests the usage of a foreign key in a 1-to-many relation.
     *
     * @throws Exception If something goes wrong in the test.
     */
    @Test
    public void oneToNTest() throws Exception {
        final DbEntity entity = dbEntity()
                .name("ACCESS_LOG")
                .addColumn("ENTRY_ID", INT, true)
                .addColumn("USER_ID", INT)
                .addFk(dbFk()
                        .addColumn("USER_ID")
                        .referencedTable("USER")
                        .addReferencedColumn("USER_ID")
                        .build()
                )
                .pkFields("ENTRY_ID")
                .build();

        checkForeignKeys(
                entity,
                // Next entry should fail, USER table doesn't contain ID 2
                entry().set("USER_ID", 2).build(),
                entry().set("USER_ID", 1).build(),
                ImmutableList.of(userEntity)
        );
    }

    /**
     * Tests the usage of a foreign key in a 1-to-many relation, when that key is composed of 2 columns.
     *
     * @throws Exception If something goes wrong in the test.
     */
    @Test
    public void multiColumnOneToNTest() throws Exception {
        final DbEntity entity = dbEntity()
                .name("ACCESS_LOG")
                .addColumn("ENTRY_ID", INT, true)
                .addColumn("USER_ID", INT)
                .addColumn("USER_AUTH_TYPE", STRING)
                .addFk(dbFk()
                        .addColumn("USER_ID", "USER_AUTH_TYPE")
                        .referencedTable("USER")
                        .addReferencedColumn("USER_ID", "AUTH_TYPE")
                        .build()
                )
                .pkFields("ENTRY_ID")
                .build();

        checkForeignKeys(
                entity,
                // USER table contains user 1, but with USER_AUTH_TYPE "LOCAL"; the FK references both columns, and since
                // their compound values don't match existing values in the referenced table, the entry below should fail
                entry().set("USER_ID", 1).set("USER_AUTH_TYPE", "EXTERNAL").build(),
                entry().set("USER_ID", 1).set("USER_AUTH_TYPE", "LOCAL").build(),
                ImmutableList.of(userEntity)
        );
    }

    /**
     * Tests the usage of a foreign key in a many-to-many relation.
     *
     * @throws Exception If something goes wrong in the test.
     */
    @Test
    public void nToNTest() throws Exception {
        final DbEntity roleEntity = dbEntity()
                .name("ROLE")
                .addColumn("ROLE_ID", INT)
                .pkFields("ROLE_ID")
                .build();

        engine.addEntity(roleEntity);
        engine.persist("ROLE", entry().set("ROLE_ID", 1).build());

        // Here we create a table for the relation user-role:
        // 1 user can have many roles, 1 role may be assigned to many users
        final DbEntity userRoleEntity = dbEntity()
                .name("USER_ROLE")
                .addColumn("USER_ID", INT)
                .addColumn("ROLE_ID", INT)
                .addFk(dbFk()
                                .addColumn("USER_ID")
                                .referencedTable("USER")
                                .addReferencedColumn("USER_ID")
                                .build(),
                        dbFk()
                                .addColumn("ROLE_ID")
                                .referencedTable("ROLE")
                                .addReferencedColumn("ROLE_ID")
                                .build()
                )
                .pkFields("USER_ID", "ROLE_ID")
                .build();

        checkForeignKeys(
                userRoleEntity,
                // USER table contains user 1, but ROLE table doesn't contain role 2, so the entry below should fail
                entry().set("USER_ID", 1).set("ROLE_ID", 2).build(),
                entry().set("USER_ID", 1).set("ROLE_ID", 1).build(),
                ImmutableList.of(userEntity, roleEntity)
        );
    }

    /**
     * Helper method to verify the behavior of an entity with a foreign key.
     * <p>
     * This method performs the following actions:
     * <ol>
     *     <li>adds the entity with a foreign key constraint to the database (the primary entities are expected
     *     to be there already)</li>
     *     <li>checks that the foreign key constraint prevents the "invalid entry" from being persisted</li>
     *     <li>persists the "valid entry"</li>
     *     <li>checks that the foreign key constraint prevents rows referenced by the "valid entry" from being deleted
     *     from the referenced entities</li>
     * </ol>
     *
     * @param fkEntity           The entity with a foreign key constraint.
     * @param invalidEntry       An entry to persist in the entity with a foreign key constraint, that should fail due
     *                           to containing values that don't match any values in the referenced primary table.
     * @param validEntry         A valid entry to persist in the entity with a foreign key constraint.
     * @param referencedEntities A list of entities that are referenced in the foreign key constraint.
     * @throws DatabaseEngineException If there is any problem adding an entity or persisting an entry.
     */
    private void checkForeignKeys(final DbEntity fkEntity,
                                  final EntityEntry invalidEntry,
                                  final EntityEntry validEntry,
                                  final List<DbEntity> referencedEntities) throws DatabaseEngineException {
        engine.addEntity(fkEntity);

        assertThatCode(() -> engine.persist(fkEntity.getName(), invalidEntry))
                .as("Persisting an entry in table '%s' should fail when the values for the columns defined" +
                                " in the foreign key don't match an existing entry from the referenced table '%s'.",
                        fkEntity.getName(),
                        referencedEntities.stream().map(DbEntity::getName).collect(Collectors.joining(", ", "[", "]"))
                )
                .isInstanceOf(DatabaseEngineException.class);

        engine.persist(fkEntity.getName(), validEntry);

        for (final DbEntity referencedEntity : referencedEntities) {
            assertThatCode(() -> engine.executeUpdate(delete(table(referencedEntity.getName()))))
                    .as("Table '%s' contains rows referenced in a foreign key, attempting to delete them should result in an error.",
                            referencedEntity.getName())
                    .isInstanceOf(DatabaseEngineException.class);
        }
    }

    /**
     * Tests that attempting to delete a row referenced by a foreign key results in an error, until the row that
     * references it is deleted.
     *
     * @throws Exception If something goes wrong in the test.
     */
    @Test
    public void removeRowPreviouslyReferencedByForeignKeyTest() throws Exception {
        final DbEntity fkEntity = dbEntity()
                .name("ACCESS_LOG")
                .addColumn("ENTRY_ID", INT, true)
                .addColumn("USER_ID", INT)
                .addFk(dbFk()
                        .addColumn("USER_ID")
                        .referencedTable("USER")
                        .addReferencedColumn("USER_ID")
                        .build()
                )
                .pkFields("ENTRY_ID")
                .build();

        engine.addEntity(fkEntity);

        engine.persist(fkEntity.getName(), entry().set("USER_ID", 1).build());

        final String userTable = userEntity.getName();
        assertThatCode(() -> engine.executeUpdate(delete(table(userTable))))
                .as("Table '%s' contains rows referenced in a foreign key, attempting to delete them should result in an error.",
                        userTable)
                .isInstanceOf(DatabaseEngineException.class);

        // Remove the row that references the row to delete
        engine.executeUpdate(delete(table(fkEntity.getName())).where(eq(column("USER_ID"), k(1))));

        assertThatCode(() -> engine.executeUpdate(delete(table(userTable))))
                .as("No rows are referencing rows from the primary table '%s', it should be possible to delete them.",
                        userTable)
                .doesNotThrowAnyException();
    }

    /**
     * Tests that attempting to delete a row referenced by a foreign key results in an error, until the foreign key is
     * removed.
     *
     * @throws Exception If something goes wrong in the test.
     */
    @Test
    public void removeRowAfterRemovingForeignKeyTest() throws Exception {
        DbEntity fkEntity = dbEntity()
                .name("ACCESS_LOG")
                .addColumn("ENTRY_ID", INT, true)
                .addColumn("USER_ID", INT)
                .addFk(dbFk()
                        .addColumn("USER_ID")
                        .referencedTable("USER")
                        .addReferencedColumn("USER_ID")
                        .build()
                )
                .pkFields("ENTRY_ID")
                .build();

        engine.addEntity(fkEntity);

        engine.persist(fkEntity.getName(), entry().set("USER_ID", 1).build());

        final String userTable = userEntity.getName();
        assertThatCode(() -> engine.executeUpdate(delete(table(userTable))))
                .as("Table '%s' contains rows referenced in a foreign key, attempting to delete them should result in an error.",
                        userTable)
                .isInstanceOf(DatabaseEngineException.class);

        // PDB property SCHEMA_POLICY must not be drop-create, otherwise the entity
        // will be dropped and the update of the FKs isn't properly tested.
        engine.close();
        properties.setProperty(SCHEMA_POLICY, "create");
        engine = DatabaseFactory.getConnection(properties);

        // Clear foreign keys from the entity
        fkEntity = fkEntity.newBuilder().clearFks().build();
        engine.updateEntity(fkEntity);

        assertThatCode(() -> engine.executeUpdate(delete(table(userTable))))
                .as("Foreign keys have been removed, it should be possible to delete rows from the primary table '%s'",
                        userTable)
                .doesNotThrowAnyException();

        assertThat(engine.query(select(all()).from(table(fkEntity.getName()))))
                .as("The table where the foreign key was previously defined should have kept its data (1 row).")
                .hasSize(1);
    }

    /**
     * Tests updating an entity in which the foreign key constraints are changed.
     * <p>
     * The test verifies the changes in foreign keys that are requested, with those keys being added/dropped only when
     * necessary.
     *
     * @throws Exception If something goes wrong in the test.
     */
    @Test
    public void updateForeignKeyTest() throws Exception {
        final DbFk fk1 = dbFk()
                .addColumn("USER_ID")
                .referencedTable("USER")
                .addReferencedColumn("USER_ID")
                .build();

        final DbFk fk2 = dbFk()
                .addColumn("USER_ID", "USER_AUTH_TYPE")
                .referencedTable("USER")
                .addReferencedColumn("USER_ID", "AUTH_TYPE")
                .build();

        DbEntity fkEntity = dbEntity()
                .name("ACCESS_LOG")
                .addColumn("ENTRY_ID", INT, true)
                .addColumn("USER_ID", INT)
                .addColumn("USER_AUTH_TYPE", STRING)
                .addFk(fk1)
                .pkFields("ENTRY_ID")
                .build();

        engine.addEntity(fkEntity);

        final LinkedList<String> fksToDrop = new LinkedList<>();
        final LinkedList<DbFk> fksToAdd = new LinkedList<>();

        /*
         The following lines implement a workaround for bugs #703 and #705 in JMockit.
         We need to capture the calls to the methods "addFks" and "dropFks", but JMockit has problems with overridden
          methods, which is the case in the CockroachDBEngine. Since the super method is called in the implementation,
          as a workaround we set the super class to be mocked instead (PostgreSqlEngine).
         */
        Class<?> dbEngineClassToMock = engine.getClass();
        while (dbEngineClassToMock.getSuperclass() != AbstractDatabaseEngine.class) {
            dbEngineClassToMock = dbEngineClassToMock.getSuperclass();
        }

        new MockUp<AbstractDatabaseEngine>(dbEngineClassToMock) {
            @Mock
            protected void dropFks(final Invocation inv, final String table, final Set<String> fks) {
                fksToDrop.addAll(fks);
                inv.proceed();
            }

            @Mock
            protected void addFks(final Invocation inv, final DbEntity entity, final Set<DbFk> fks) {
                fksToAdd.addAll(fks);
                inv.proceed();
            }
        };

        // Update 1: the entity has no changes, but since the schema policy is 'drop-create'
        // the table will be created again along with foreign keys
        engine.updateEntity(fkEntity);

        assertThat(fksToDrop)
                .as("When updating an entity with 'drop-create', it is first dropped along with its constraints, no need to drop FKs.")
                .isEmpty();
        assertThat(fksToAdd)
                .as("When updating an entity with 'drop-create', it is first dropped and then created, so the FKs need to be created as well.")
                .containsExactly(fk1);

        fksToAdd.clear();

        // PDB property SCHEMA_POLICY must not be drop-create, otherwise the entity
        // will be dropped and the update of the FKs isn't properly tested.
        engine.close();
        properties.setProperty(SCHEMA_POLICY, "create");
        engine = DatabaseFactory.getConnection(properties);

        // Update 2: the entity has no changes, so with schema policy is 'create' nothing should be done
        engine.updateEntity(fkEntity);

        assertThat(fksToDrop)
                .as("When updating an entity with 'create' policy, FKs shouldn't be dropped if the table already matches the entity.")
                .isEmpty();
        assertThat(fksToAdd)
                .as("When updating an entity with 'create' policy, no FKs should be added if the table already matches the entity.")
                .isEmpty();

        fkEntity = fkEntity.newBuilder()
                .clearFks()
                .addFk(fk2)
                .build();

        // Update 3: the foreign key has been replaced in the entity, old one should be removed and the new one added
        engine.updateEntity(fkEntity);

        assertThat(fksToDrop)
                .as("When updating an entity by replacing a FK, the old FK should be dropped.")
                .hasSize(1);
        assertThat(fksToAdd)
                .as("When updating an entity by replacing a FK, the new FK should be added.")
                .containsExactly(fk2);
    }
}
