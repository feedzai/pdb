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
package com.feedzai.commons.sql.abstraction.engine.handler;

import java.io.Serializable;

/**
 * Interface to control the definition flow when defining new entities.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public interface ExceptionHandler extends Serializable {
    /**
     * Default exception handler that doesn't stop the definition flow
     * in any case.
     */
    public static final ExceptionHandler DEFAULT = new ExceptionHandler() {
        @Override
        public boolean proceed(OperationFault op, Exception e) {
            return true;
        }
    };

    /**
     * Decides if the flow must continue after a faulty operation take place.
     *
     * @param op The operation context that originated the exception.
     * @param e  The exception generated during the operation.
     * @return {@code true} if the flow is to be continued ignoring this exception, {@code false} otherwise.
     */
    boolean proceed(OperationFault op, Exception e);
}
