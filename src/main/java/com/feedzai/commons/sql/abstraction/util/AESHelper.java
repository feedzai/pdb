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

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;

/**
 * Class to provide encryption and decryption using AES algorithm.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public final class AESHelper {
    /**
     * The logger.
     */
    private static Logger logger = LoggerFactory.getLogger(AESHelper.class);

    /**
     * Encrypts a string.
     *
     * @param c The string to encrypt.
     * @return The encrypted string in HEX.
     */
    public static String encrypt(String c, String key) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(Hex.decodeHex(key.toCharArray()), "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
            byte[] encoded = cipher.doFinal(c.getBytes());
            return new String(Hex.encodeHex(encoded));

        } catch (Exception e) {
            logger.warn("Could not encrypt string", e);
            return null;
        }
    }

    /**
     * Encrypts a byte[].
     *
     * @param c   The byte[] to encrypt.
     * @param key The key.
     * @return The encrypted array as a HEX string.
     */
    public static String encrypt(byte[] c, String key) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(Hex.decodeHex(key.toCharArray()), "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
            byte[] encoded = cipher.doFinal(c);
            return new String(Hex.encodeHex(encoded));

        } catch (Exception e) {
            logger.warn("Could not encrypt byte[]", e);
            return null;
        }
    }

    /**
     * Decrypts a string encrypted by {@link #encrypt} method.
     *
     * @param c   The encrypted HEX string.
     * @param key The  key.
     * @return The decrypted string.
     */
    public static String decrypt(String c, String key) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(Hex.decodeHex(key.toCharArray()), "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);
            byte[] decoded = cipher.doFinal(Hex.decodeHex(c.toCharArray()));
            return new String(decoded);
        } catch (Exception e) {
            logger.warn("Could not decrypt string", e);
            return null;
        }
    }

    /**
     * Decrypts a byte[] encrypted by {@link #encrypt} method.
     *
     * @param c   The encrypted HEX byte[].
     * @param key The key.
     * @return The decrypted string in a byte[].
     */
    public static byte[] decrypt(byte[] c, String key) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(Hex.decodeHex(key.toCharArray()), "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);
            return cipher.doFinal(Hex.decodeHex((new String(c).toCharArray())));
        } catch (Exception e) {
            logger.warn("Could not decrypt byte[]", e);
            return null;
        }
    }

    /**
     * Decrypts a file encrypted by {@link #encryptToFile(String, byte[], String)} method.
     *
     * @param path The file path to decrypt.
     * @param key  The key.
     * @return A decrypted byte[].
     */
    public static byte[] decryptFile(String path, String key) {
        try {
            byte[] buf = readFile(path);
            return decrypt(buf, key);
        } catch (Exception e) {
            logger.warn("Could not decrypt file {}", path, e);
            return null;
        }
    }

    /**
     * Encrypts the byte[] to a file.
     *
     * @param path The destination path.
     * @param buf  The buffer.
     * @param key  The key.
     */
    public static void encryptToFile(String path, byte[] buf, String key) {
        try {
            FileOutputStream fos = new FileOutputStream(path);
            fos.write(encrypt(buf, key).getBytes());
            fos.close();
        } catch (Exception e) {
            logger.warn("Could not encrypt to file {}", path, e);
        }
    }

    /**
     * Reads a file.
     *
     * @param filePath The file path.
     * @return a byte[] The file data.
     * @throws java.io.IOException if an error occurs reading the file or if the file does not exists.
     */
    public static byte[] readFile(String filePath) throws IOException {
        byte[] buffer = new byte[(int) new File(filePath).length()];
        BufferedInputStream f = null;
        try {
            f = new BufferedInputStream(new FileInputStream(filePath));
            f.read(buffer);
        } finally {
            if (f != null) {
                try {
                    f.close();
                } catch (IOException ignored) {
                }
            }
        }
        return buffer;
    }
}
