/*
 * Copyright 2014 Feedzai
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
package com.feedzai.commons.sql.abstraction.ddl;

import com.feedzai.commons.sql.abstraction.dml.Expression;

/**
 * Expression to translate {@code ALTER TABLE... ALTER COLUMN...} statements.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class AlterColumn extends Expression {

    /**
     * the table where the column resides
     */
    private final Expression table;
    /**
     * the column definition
     */
    private final DbColumn column;

    /**
     * Creates a new instance of {@link AlterColumn}.
     *
     * @param table  The table where the column resides.
     * @param column The column definition.
     */
    public AlterColumn(Expression table, DbColumn column) {
        this.table = table;
        this.column = column;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }

    /**
     * Gets the table expression.
     *
     * @return The table expression.
     */
    public Expression getTable() {
        return table;
    }

    /**
     * Gets the column to change.
     *
     * @return The column to change.
     */
    public DbColumn getColumn() {
        return column;
    }
}
