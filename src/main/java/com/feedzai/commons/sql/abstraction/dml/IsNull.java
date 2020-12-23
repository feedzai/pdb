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
package com.feedzai.commons.sql.abstraction.dml;

/**
 * PDB Is Null Keyword.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since @@@feedzai.next.release@@@
 */
public class IsNull extends Expression {

    /**
     * The column to check if it's null.
     */
    private final Expression column;

    /**
     * Creates a new {@link IsNull} instance.
     *
     * @param column the column to check if it's null.
     */
    public IsNull(final Expression column) {
        this.column = column;
    }

    /**
     * Gets the column to check if it's null.
     *
     * @return the column to check if it's null.
     */
    public Expression getColumn() {
        return column;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
