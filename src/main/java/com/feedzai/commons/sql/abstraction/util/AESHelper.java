/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.util;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.security.NoSuchAlgorithmException;

/**
 * Class to provide encryption and decryption using AES algorithm.
 */
public final class AESHelper {
    /*
     * The Options. 
     */
    private static final String GENKEY = "g";
    private static final String ENCRYPT = "e";
    private static final String DECRYPT = "d";
    private static final String GENKEY_LONG = "genkey";
    private static final String ENCRYPT_LONG = "encrypt";
    private static final String DECRYPT_LONG = "decrypt";
    private static final String HELP = "h";
    private static final String HELP_LONG = "help";
    private static final String LAUNCH_STRING = "AESHelper";
    private static Logger logger =  LoggerFactory.getLogger(AESHelper.class);

    /**
     * Encrypts a string.
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
            logger.warn("Could not encrypt string",e);
            return null;
        }
    }

    /**
     * Encrypts a byte[]
     * @param c The byte[] to encrypt.
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
            logger.warn("Could not encrypt byte[]",e);
            return null;
        }
    }

    /**
     * Decrypts a string encrypted by {@link #encrypt} method.
     * @param c The encrypted HEX string.
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
            logger.warn("Could not decrypt string",e);
            return null;
        }
    }

    /**
     * Decrypts a byte[] encrypted by {@link #encrypt} method.
     * @param c The encrypted HEX byte[].
     * @return The decrypted string in a byte[].
     */
    public static byte[] decrypt(byte[] c, String key) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(Hex.decodeHex(key.toCharArray()), "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);
            return cipher.doFinal(Hex.decodeHex((new String(c).toCharArray())));
        } catch (Exception e) {
            logger.warn("Could not decrypt byte[]",e);
            return null;
        }
    }

    /**
     * Decrpts a encrypted file.
     * @param path The file path to decrypt.
     * @return A decrypted byte[].
     */
    public static byte[] decryptFile(String path, String key) {
        try {
            byte[] buf = readFile(path);
            return decrypt(buf, key);
        } catch (Exception e) {
            logger.warn("Could not decrypt file {}",path,e);
            return null;
        }
    }

    /**
     *
     * @param path
     * @param buf
     */
    public static void encryptToFile(String path, byte[] buf, String key) {
        try {
            FileOutputStream fos = new FileOutputStream(path);
            fos.write(encrypt(buf, key).getBytes());
            fos.close();
        } catch (Exception e) {
            logger.warn("Could not encrypt to file {}",path,e);
        }
    }

    /**
     * Reads a file.
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

    public static String generateKey() {
        KeyGenerator kgen;
        try {
            kgen = KeyGenerator.getInstance("AES");
            kgen.init(128); // 192 and 256 bits may not be available


            // Generate the secret key specs.
            SecretKey skey = kgen.generateKey();

            return new String(Hex.encodeHex(skey.getEncoded()));
        } catch (NoSuchAlgorithmException ex) {
            return null;
        }
    }
}
