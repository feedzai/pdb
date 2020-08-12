/*
 * Copyright 2020 Feedzai
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
package com.feedzai.commons.sql.abstraction.dml.result;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

/**
 * Class for testing the result iterator behaviour.
 *
 * @author Helena Poleri (helena.poleri@feedzai.com)
 * @since 2.7.1
 */
public class ResultIteratorTest {

    /**
     * Assesses whether the current row count is incremented if the .next()/.nextResult()
     * methods are called in the iterator.
     *
     * @param statement Mocked statement.
     * @param resultSet Mocked result set.
     * @throws DatabaseEngineException If a database access error happens.
     * @throws SQLException If a database access error happens.
     */
    @Test
    public void doesRowCountIncrementTest(@Mocked Statement statement,
                                          @Mocked ResultSet resultSet)
            throws DatabaseEngineException, SQLException {

        final ResultIterator resultIterator = new MySqlResultIterator(statement, null);

        new Expectations() {{
            resultSet.next(); result = true;
        }};

        assertEquals("The current row count should be 0 if the iteration hasn't started",
                0,
                resultIterator.getCurrentRowCount());

        // If the .next() method is called 5 times then the current row count
        // should be updated to 5
        for(int i = 0; i < 5; i++) {
            resultIterator.next();
        }

        assertEquals("The current row count is equal to 5",
                5,
                resultIterator.getCurrentRowCount());

        // If for the same iterator the .nextResult() method is called 3 additional
        // times then the current row count should be updated to 8
        for(int i = 0; i < 3; i++) {
            resultIterator.nextResult();
        }

        assertEquals("The current row count is equal to 8",
                8,
                resultIterator.getCurrentRowCount());
    }
}
