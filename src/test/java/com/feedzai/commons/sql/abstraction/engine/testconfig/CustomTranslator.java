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
package com.feedzai.commons.sql.abstraction.engine.testconfig;

import com.feedzai.commons.sql.abstraction.ddl.AlterColumn;
import com.feedzai.commons.sql.abstraction.ddl.DbColumn;
import com.feedzai.commons.sql.abstraction.ddl.DropPrimaryKey;
import com.feedzai.commons.sql.abstraction.ddl.Rename;
import com.feedzai.commons.sql.abstraction.dml.*;
import com.feedzai.commons.sql.abstraction.engine.AbstractTranslator;

/**
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class CustomTranslator extends AbstractTranslator {

    @Override
    public String translateEscape() {
        return null;
    }

    @Override
    public String translateTrue() {
        return null;
    }

    @Override
    public String translateFalse() {
        return null;
    }

    @Override
    public String translate(AlterColumn ac) {
        return null;
    }

    @Override
    public String translate(DropPrimaryKey dpk) {
        return null;
    }

    @Override
    public String translate(Function f) {
        return null;
    }

    @Override
    public String translate(Modulo m) {
        return null;
    }

    @Override
    public String translate(Rename r) {
        return null;
    }

    @Override
    public String translate(RepeatDelimiter rd) {
        return null;
    }

    @Override
    public String translate(Query q) {
        return null;
    }

    @Override
    public String translate(View v) {
        return null;
    }

    @Override
    public String translate(DbColumn dc) {
        return null;
    }
}
