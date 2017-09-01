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
     * Generates de MD5 checksum for the specified message.
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
        return md5(message).substring(0, nchar);
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
}
