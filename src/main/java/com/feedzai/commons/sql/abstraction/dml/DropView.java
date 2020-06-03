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

import com.feedzai.commons.sql.abstraction.util.StringUtils;

/**
 * Represents a SQL drop view.
 *
 * @author Nuno Santos (nuno.santos@feedzai.com)
 * @since 2.5.3
 */
public class DropView extends Expression {

    /**
     * The name of the view to be dropped.
     */
    private final String name;

    /**
     * Creates a new instance of {@link DropView}.
     *
     * @param name The name of the view to be dropped.
     */
    public DropView(final String name) {
        this.name = StringUtils.escapeSql(name);
    }

    /**
     * Gets the view name.
     *
     * @return The view name.
     */
    public String getName() {
        return name;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
