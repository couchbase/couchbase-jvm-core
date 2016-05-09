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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;

/**
 * A {@link ByteBufProcessor} to find the position of a single character usable as a split pivot.
 * Applying this to a buffer will output the position of the closing of the section, relative to that buffer's
 * readerIndex, or -1 if the end of the section couldn't be found.
 *
 * Note that this processor is typically used to find split positions in a JSON array inside a streaming JSON
 * response (in which case the constructor variant that detects JSON strings should be used).
 *
 * It is invoked on a {@link ByteBuf} by calling {@link ByteBuf#forEachByte(ByteBufProcessor)} methods.
 *
 * @author Simon Baslé
 * @since 1.3
 */
public class SplitPositionBufProcessor extends AbstractStringAwareBufProcessor implements ByteBufProcessor {

    /**
     * The character to search for.
     */
    private final char splitChar;

    /**
     * Should we detect opening and closing of JSON strings and ignore characters in there?
     */
    private final boolean detectJsonString;

    /**
     * @param splitChar the split character to find.
     */
    public SplitPositionBufProcessor(char splitChar) {
        this(splitChar, false);
    }

    /**
     * @param splitChar the split character to find.
     * @param detectJsonString set to true to not inspect bytes detected as being part of a String.
     */
    public SplitPositionBufProcessor(char splitChar, boolean detectJsonString) {
        this.splitChar = splitChar;
        this.detectJsonString = detectJsonString;
    }

    @Override
    public boolean process(final byte current) throws Exception {
        //don't look into escaped bytes for opening/closing section
        if (detectJsonString && isEscaped(current)) {
            return true;
        }

        if (current == splitChar) {
            return false;
        }
        return true;
    }
}