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
package com.feedzai.commons.sql.abstraction.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.apache.commons.lang3.StringUtils.replace;

/**
 * String Utilities.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class StringUtils {
    /**
     * Puts quotes around a String.
     *
     * @param s the string to be 'quotized'
     * @return a the string with quotes
     */
    public static String quotize(String s) {
        return quotize(s, "\"");
    }

    /**
     * Puts quotes around a String.
     *
     * @param s the string to be 'quotized'
     * @return a the string with quotes
     */
    public static String quotize(String s, String quoteChar) {
        return quoteChar + s + quoteChar;
    }

    /**
     * Puts single quotes around a string.
     *
     * @param s the string to be single 'quotized'
     * @return a string with single quotes
     */
    public static String singleQuotize(String s) {
        return quotize(s, "'");
    }

    /**
     * Generates the MD5 checksum for the specified message.
     *
     * @param message The message.
     * @return The hexadecimal checksum.
     */
    public static String md5(final String message) {
        byte[] res;

        try {
            MessageDigest instance = MessageDigest.getInstance("MD5");
            instance.reset();
            instance.update(message.getBytes());
            res = instance.digest();

        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }

        StringBuilder hexString = new StringBuilder();
        for (byte resByte : res) {
            hexString.append(Integer.toString((resByte & 0xff) + 0x100, 16).substring(1));
        }

        return hexString.toString();
    }

    /**
     * Generates de MD5 checksum for the specified message.
     *
     * @param message The message.
     * @param nchar   The maximum number of chars for the result hash.
     * @return The hexadecimal checksum with the specified maximum number of chars.
     */
    public static String md5(final String message, final int nchar) {
        final String hash = md5(message);
        return nchar > hash.length() ? hash : hash.substring(0, nchar);
    }

    /**
     * Reads a string from the input stream.
     *
     * @param stream The stream.
     * @return The string from that stream.
     * @throws IOException If an I/O error occurs.
     */
    public static String readString(final InputStream stream) throws IOException {
        InputStreamReader br = null;
        StringBuilder sb = new StringBuilder();
        try {
            br = new InputStreamReader(stream);

            int got;
            while (!Thread.currentThread().isInterrupted()) {
                got = br.read();
                if (got == -1) {
                    break;
                }

                sb.append((char) got);
            }
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ex) {
                }
            }

        }

        return sb.toString();
    }

    /**
     * Apache Commons-Lang 2.X contained StringEscapeUtils#escapeSql, but this method was removed in 3.X as discussed
     * here: https://commons.apache.org/proper/commons-lang/article3_0.html#StringEscapeUtils.escapeSql
     *
     * For this reason, the source code has been copied from 2.X to here so that we can continue to use the logic and
     * possibly build on it in the future.
     *
     * ***************** Copied from commons-lang:commons-lang:2.6 ****************
     *
     * <p>Escapes the characters in a <code>String</code> to be suitable to pass to
     * an SQL query.</p>
     *
     * <p>For example,
     * <pre>statement.executeQuery("SELECT * FROM MOVIES WHERE TITLE='" +
     *   StringEscapeUtils.escapeSql("McHale's Navy") +
     *   "'");</pre>
     * </p>
     *
     * <p>At present, this method only turns single-quotes into doubled single-quotes
     * (<code>"McHale's Navy"</code> => <code>"McHale''s Navy"</code>). It does not
     * handle the cases of percent (%) or underscore (_) for use in LIKE clauses.</p>
     *
     * see http://www.jguru.com/faq/view.jsp?EID=8881
     * @param str  the string to escape, may be null
     * @return a new String, escaped for SQL, <code>null</code> if null string input
     */
    public static String escapeSql(String str) {
        if (str == null) {
            return null;
        }
        return replace(str, "'", "''");
    }
}
