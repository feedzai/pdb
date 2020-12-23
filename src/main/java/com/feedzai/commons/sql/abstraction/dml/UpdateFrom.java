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
 * PDB Update From Operator.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since @@@feedzai.next.release@@@
 */
public class UpdateFrom extends Update {

    /**
     * The from clause.
     */
    private Expression from;

    /**
     * Creates a new {@link UpdateFrom} instance.
     *
     * @param table the table to update.
     */
    public UpdateFrom(final Expression table) {
        super(table);
    }

    /**
     * Sets the from clause.
     *
     * @param from the from clause.
     * @return this instance.
     */
    public UpdateFrom from(final Expression from) {
        this.from = from;
        return this;
    }

    /**
     * Gets the from clause.
     *
     * @return the from clause.
     */
    public Expression getFrom() {
        return from;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
