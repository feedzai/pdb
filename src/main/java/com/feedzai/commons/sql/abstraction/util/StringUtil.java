/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.util;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Iterator;

/**
 * A set of functions to manipulate Strings.
 */
public final class StringUtil {

    /**
     * Disable the default constructor.
     */
    private StringUtil() {
    }

    /**
     * Escapes a String for SQL usage.
     *
     * @param s the string to be 'escaped'
     * @return a the string with escaped
     */
    public static String escapeSql(String s) {
        return StringEscapeUtils.escapeSql(s);
    }

    /**
     * Strips a String to Null.
     *
     * @param s the string to be 'striped'
     * @return a the string striped
     */
    public static String stripToNull(String s) {
        return StringUtils.stripToNull(s);
    }

    /**
     * Tests if a String is blank.
     *
     * @param s the string to be tested
     * @return true if s is blank and false otherwise
     */
    public static Boolean isBlank(String s) {
        return StringUtils.isBlank(s);
    }

    /**
     * Tests if a String is empty.
     *
     * @param s the string to be tested
     * @return true if s is empty and false otherwise
     */
    public static Boolean isEmpty(String s) {
        return StringUtils.isEmpty(s);
    }

    /**
     * Puts quotes around a String.
     *
     * @param s the string to be 'quotized'
     * @return a the string with quotes
     */
    public static String quotize(String s) {
        return "\"" + s + "\"";
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
     * @param s the string to be single 'quotized'
     * @return a string with single quotes
     */
    public static String singleQuotize(String s) {
        return "'" + s + "'";
    }

    /**
     * Reads a string from the input stream.
     * @param stream The stream.
     * @return The string from that stream.
     * @throws java.io.IOException If an I/O error occurs.
     */
    public static String readString(final InputStream stream) throws IOException {
        InputStreamReader br = null;
        StringBuilder sb = new StringBuilder();
        try {
            br = new InputStreamReader(stream);

            int got;
            while (true) {
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
     * Joins the collection elements as a string.
     * @param <T> The type of the collection elements.
     * @param collection The collection.
     * @return The collection elements joined as a string.
     */
    public static <T> String join(final Collection<T> collection) {
        return join(collection, "");
    }

    /**
     * Joins the collection elements as a string.
     * @param <T> The type of the collection elements.
     * @param collection The collection.
     * @param separator The separator between each collection element;
     * @return The collection elements joined as a string.
     */
    public static <T> String join(final Collection<T> collection, String separator) {
        StringBuilder builder = new StringBuilder();

        if (separator == null) {
            separator = "";
        }

        Iterator<T> it = collection.iterator();
        while (it.hasNext()) {
            builder.append(it.next());
            if (it.hasNext()) {
                builder.append(separator);
            }
        }

        return builder.toString();
    }
}
