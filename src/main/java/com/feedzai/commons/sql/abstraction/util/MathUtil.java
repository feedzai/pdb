/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A set of math related functions.
 */
public final class MathUtil {

    /**
     * Disable the default constructor.
     */
    private MathUtil() {
    }

    /**
     * Generates de MD5 checksum for the specified message.
     * @param message The message.
     * @return The hexadecimal checksum.
     */
    public static String md5(final String message) {
        byte []res;

        try {
            MessageDigest instance = MessageDigest.getInstance("MD5");
            instance.reset();
            instance.update(message.getBytes());
            res = instance.digest();

        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }

        StringBuffer hexString = new StringBuffer();
        for (int i = 0; i < res.length; i++) {
            hexString.append(Integer.toString((res[i] & 0xff) + 0x100, 16).substring(1));

        }

        return hexString.toString();
    }

    /**
     * Generates de MD5 checksum for the specified message.
     * @param message The message.
     * @param nchar The maximum number of chars for the result hash.
     * @return The hexadecimal checksum with the specified maximum number of chars.
     */
    public static String md5(final String message, final int nchar) {
        return md5(message).substring(0, nchar);
    }
}
