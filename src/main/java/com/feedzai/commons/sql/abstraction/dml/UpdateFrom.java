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
    private final Expression from;

    /**
     * Creates a new {@link UpdateFrom} instance.
     *
     * @param table the table to update.
     * @param from the from.
     * @implNote Apart from PostgreSQL, engines do not support this operator, so the final SQL will be different.
     * @implNote Check https://stackoverflow.com/a/44845278
     */
    public UpdateFrom(final Expression table, final Expression from) {
        super(table);
        this.from = from;
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
