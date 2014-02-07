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
 * Expression to rename tables.
 *
 * @author Diogo Ferreira (diogo.ferreira@feedzai.com)
 * @since 2.0.0
 */
public class Rename extends Expression {
    /**
     * The old name of the table.
     */
    private final Expression oldName;
    /**
     * The new name of the table.
     */
    private final Expression newName;

    /**
     * Creates a new instance of {@link Rename}.
     *
     * @param oldName The old table name.
     * @param newName The new table name.
     */
    public Rename(final Expression oldName, final Expression newName) {
        this.oldName = oldName;
        this.newName = newName;
    }

    /**
     * Gets the old name.
     *
     * @return The old name.
     */
    public Expression getOldName() {
        return oldName;
    }

    /**
     * Gets the new name.
     *
     * @return The new name.
     */
    public Expression getNewName() {
        return newName;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
