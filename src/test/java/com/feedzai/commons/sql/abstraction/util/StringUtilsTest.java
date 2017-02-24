/*
 * Copyright 2017 Feedzai
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
package com.feedzai.commons.sql.abstraction.util;

import com.google.common.base.Charsets;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests functions from {@link StringUtils}.
 *
 * @author Case Walker (case.walker@feedzai.com)
 * @since 2.1.12
 */
public class StringUtilsTest {
    /**
     * Test that quotizing returns a double-quoted string.
     */
    @Test
    public void quotize() {
        assertEquals("Quotizing a string should add double quotes",
                "\"string\"",
                StringUtils.quotize("string"));
    }

    /**
     * Test that single-quotizing returns a single-quoted string.
     */
    @Test
    public void singleQuotize() {
        assertEquals("SingleQuotizing a string should add single quotes",
                "'string'",
                StringUtils.singleQuotize("string"));
    }

    /**
     * Test that md5 returns the expected md5 calculation.
     */
    @Test
    public void md5() {
        assertEquals("md5 should create the right output",
                "b45cffe084dd3d20d928bee85e7b0f21",
                StringUtils.md5("string"));
    }

    /**
     * Test that readString can read from an input stream.
     *
     * @throws IOException if there is a problem with readString.
     */
    @Test
    public void readStringTest() throws IOException {
        String string = "Lorem ipsum dolor sit amet, consectetur adipiscing elit.";

        assertEquals("readString should get the whole input",
                string,
                StringUtils.readString(new ByteArrayInputStream(string.getBytes(Charsets.US_ASCII))));
    }

    /**
     * Test that escapeSql handles null input.
     */
    @Test
    public void escapeSqlNull() {
        assertNull("escapeSql should handle null", StringUtils.escapeSql(null));
    }


    /**
     * Test that escapeSql escapes single quotes.
     */
    @Test
    public void escapeSql() {
        assertEquals("escapeSql should handle single-quotes",
                "McHale''s Navy",
                StringUtils.escapeSql("McHale's Navy"));
    }
}
