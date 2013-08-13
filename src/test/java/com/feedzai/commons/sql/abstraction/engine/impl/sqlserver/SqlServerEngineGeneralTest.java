/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.impl.sqlserver;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineGeneralTest;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.TestConfiguration;
import org.junit.Test;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;

public class SqlServerEngineGeneralTest extends AbstractEngineGeneralTest {

    @Override
    protected String getEngine() {
        return "com.feedzai.commons.sql.abstraction.engine.impl.SqlServerEngine";
    }

    @Test
    public void selectFromWithNoLockTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all()).from(table("TEST").withNoLock()));
    }

    @Test
    public void selectFromWithNoLockQWithAliasTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all()).from(table("TEST").alias("ALIAS").withNoLock()));
    }

    @Test
    public void joinsWithNoLocksTest() throws DatabaseEngineException {
        userRolePermissionSchema();

        engine.query(
                select(all()).from(
                table("USER").alias("a").withNoLock().innerJoin(table("USER_ROLE").alias("b").withNoLock(), eq(column("a", "COL1"), column("b", "COL1")))));

        engine.query(
                select(all()).from(
                table("USER").alias("a").withNoLock().innerJoin(table("USER_ROLE").alias("b").withNoLock(), eq(column("a", "COL1"), column("b", "COL1"))).innerJoin(table("ROLE").alias("c"), eq(column("b", "COL2"), column("c", "COL1")))));

        engine.query(
                select(all()).from(
                table("USER").alias("a").withNoLock().rightOuterJoin(table("USER_ROLE").alias("b").withNoLock(), eq(column("a", "COL1"), column("b", "COL1")))));

        engine.query(
                select(all()).from(
                table("USER").alias("a").withNoLock().leftOuterJoin(table("USER_ROLE").alias("b").withNoLock(), eq(column("a", "COL1"), column("b", "COL1")))));
    }

    protected TestConfiguration.Configuration.Database getDatabaseType() { return TestConfiguration.Configuration.Database.SQLSERVER; }
}
