/*
 * Copyright 2015 Feedzai
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
package com.feedzai.commons.sql.abstraction.engine;

import com.feedzai.commons.sql.abstraction.util.InitiallyReusableByteArrayOutputStream;
import com.google.common.io.ByteStreams;

import java.io.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Logic to encode java values into blob values and vice-versa. BLOB values are
 * stored with a byte prefix, the encoder id, followed by the encoded value. The
 * encoding to use is the most efficient for the type of the java value to store.
 *
 * @author  Paulo Leitao (paulo.leitao@feedzai.com)
 * @since 2.1.5
 */
public class BlobEncoder {

    /**
     * Prefix for objects encoded with java serialization, set with the same value as the first byte
     * of STREAM_MAGIC in the java serialization protocol (java serialized objects always start by AC ED).
     * In order to maintain backward compatibility, this prefix is part of the serialized value.
     */
    private static final int ID_JAVA_ENCODER = 0xac;
    /**
     * Prefix for objects not serialized.
     */
    private static final int ID_NULL_ENCODER = 0x00;

    /**
     * Size of blob buffer.
     */
    private int blobBufferSize;
    /**
     * The reusable initial byte buffer for blob values, allocated only if needed.
     */
    private byte[] reusableByteBuffer;
    /**
     * Lock to control access to reusable buffer
     */
    private final Lock bufferLock = new ReentrantLock();

    /**
     * Create new encoder.
     *
     * @param blobBufferSize  Encoder internal buffer size, the maximum size of a blob value.
     */
    public BlobEncoder(int blobBufferSize) {
        this.blobBufferSize = blobBufferSize;
    }

    /**
     * Converts an object to a byte array representation in a BLOB value.
     *
     * @param val The object to convert.
     * @return The byte array representation of the object.
     * @throws IOException If the buffer is not enough to make the conversion.
     */
    public final byte[] encode(Object val) throws IOException {
        if (val == null) {
            return null;
        }
        if (val instanceof  byte[]) {
            // Byte arrays can be saved with no serialization, just add encoder id
            byte[] arrayValue = (byte[]) val;
            byte[] encodedValue = new byte[arrayValue.length + 1];
            encodedValue[0] = ID_NULL_ENCODER;
            System.arraycopy(arrayValue, 0, encodedValue, 1, arrayValue.length);
            return encodedValue;

        } else {
            // All other types are encoded with standard java serialization
            bufferLock.lock();
            try {
                final ByteArrayOutputStream bos = new InitiallyReusableByteArrayOutputStream(getReusableByteBuffer());
                final ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(val);

                return bos.toByteArray();
            } finally {
                bufferLock.unlock();
            }
        }
    }

    /**
     * Converts a blob representation into its java representation.
     *
     * @param is  InputStream with the blob value, typically obtained with jdbc Blob.getInputStream().
     * @return   The correspondent java object.
     */
    public final static Object decode(InputStream is) {
        try {
            // We need a buffered input stream because jdbc input streams may not support mark/reset
            is = new BufferedInputStream(is, 1);

            // Get the encoder id
            is.mark(1);
            int encoderId = is.read();

            // Decode according to the decoder id
            switch(encoderId) {
                case ID_JAVA_ENCODER:
                    // Put encoder id back in the stream and deserialize
                    is.reset();
                    ObjectInputStream ois = new ObjectInputStream(is);
                    return ois.readObject();

                case ID_NULL_ENCODER:
                    // Value is a byte array and is stored right after the encoder id
                    return ByteStreams.toByteArray(is);

                default:
                    throw new DatabaseEngineRuntimeException("Invalid encoder ID, BLOB value is corrupted! ");
            }

        } catch (Exception e) {
            throw new DatabaseEngineRuntimeException("Error converting blob to object", e);
        }
    }

    /**
     * Obtains the reusable byte buffer. The reusable byte buffer is allocated in the first call. Proper synchronization
     * is required.
     *
     * @return A byte array shared amongst all threads.
     */
    private byte[] getReusableByteBuffer() {
        if (reusableByteBuffer == null) {
            reusableByteBuffer = new byte[blobBufferSize];
        }
        return reusableByteBuffer;
    }

}
