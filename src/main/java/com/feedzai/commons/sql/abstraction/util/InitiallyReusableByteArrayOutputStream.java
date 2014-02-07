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

import java.io.ByteArrayOutputStream;

/**
 * A take on {@link ByteArrayOutputStream} that takes a provided byte array as its
 * initial buffer. This is useful for situations where there is only one thread at
 * a time creating these objects and allocating the initial array is expensive.
 *
 * @author Diogo Ferreira (diogo.ferreira@feedzai.com)
 * @since 2.0.0
 */
public final class InitiallyReusableByteArrayOutputStream extends ByteArrayOutputStream {
    /**
     * Builds an instance with an initial buffer.
     *
     * @param initialArray The initial buffer array.
     */
    public InitiallyReusableByteArrayOutputStream(byte[] initialArray) {
        this.buf = initialArray;
    }
}
