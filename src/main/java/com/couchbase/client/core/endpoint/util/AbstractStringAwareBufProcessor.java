/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.endpoint.util;

/**
 * Base class for {@link io.netty.buffer.ByteBufProcessor} that need to take JSON string escaping
 * into account, through the {@link #isEscaped(byte)} method.
 *
 * @author Simon Baslé
 * @since 1.3
 */
public abstract class AbstractStringAwareBufProcessor {

    /**
     * Boolean to indicate we are reading escaped character
     */
    private boolean escapedInString = false;

    /** flag to indicate that we are currently reading a JSON string */
    private boolean inString = false;

    /**
     * Detects opening and closing of JSON strings and keep track of it in order
     * to mark characters in the string (delimiter quotes included) as escaped.
     *
     * Quotes escaped by a \ are correctly detected and do not mark a closing of
     * a JSON string.
     *
     * @param nextByte the next byte to inspect.
     * @return true if the byte should be ignored as part of a JSON string, false otherwise.
     */
    protected boolean isEscaped(byte nextByte) {
        boolean result = false;
        if (inString) {
            if (nextByte == '\"' && !escapedInString) {
                inString = false;
            }

            if (nextByte == '\\' && !escapedInString) {
                escapedInString = true;
            } else if (escapedInString) {
                escapedInString = false;
            }
            result = true;
        } else {
            if (nextByte == '\"') {
                inString = true;
                result = true;
            }
        }
        return result;
    }
}
