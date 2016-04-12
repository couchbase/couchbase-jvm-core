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
 * A {@link ByteBufProcessor} to find a closing character position. Applying this to a
 * buffer will output the position of the closing of the section, relative to that buffer's
 * readerIndex, or -1 if the end of the section couldn't be found.
 *
 * Note that this processor will only work correctly if the number of opening and closing
 * characters match up. This is typically the case when searching for open and closing {}
 * in a streaming JSON response (in which case the constructor variant that detects JSON
 * strings should be used).
 *
 * It is invoked on a {@link ByteBuf} by calling {@link ByteBuf#forEachByte(ByteBufProcessor)} methods.
 *
 * @author Simon Baslé
 * @since 1.1.0
 */
public class ClosingPositionBufProcessor extends AbstractStringAwareBufProcessor implements ByteBufProcessor {

    /**
     * The number of open characters found so far.
     */
    private int openCount = 0;

    /**
     * The open character to search for.
     */
    private final char openingChar;

    /**
     * The close character to search for.
     */
    private final char closingChar;

    /**
     * Should we detect opening and closing of JSON strings and ignore characters in there?
     */
    private final boolean detectJsonString;

    /**
     * @param openingChar the opening section character (used to detect a sub-section).
     * @param closingChar the closing section character to search for.
     */
    public ClosingPositionBufProcessor(char openingChar, char closingChar) {
        this(openingChar, closingChar, false);
    }

    /**
     * @param openingChar the opening section character (used to detect a sub-section)
     * @param closingChar the closing section character to search for.
     * @param detectJsonString set to true to not inspect bytes detected as being part of a String.
     */
    public ClosingPositionBufProcessor(char openingChar, char closingChar, boolean detectJsonString) {
        if (openingChar == closingChar) {
            throw new IllegalArgumentException("only asymmetric section enclosing characters are supported");
        }

        this.openingChar = openingChar;
        this.closingChar = closingChar;
        this.detectJsonString = detectJsonString;
    }

    @Override
    public boolean process(final byte current) throws Exception {
        //don't look into escaped bytes for opening/closing section
        //note that we wait for the opening of the section to do string checking
        if (detectJsonString && openCount > 0 && isEscaped(current)) {
            return true;
        }

        //now lookout for sub-sections, try to find the closing of the root section
        if (current == openingChar) {
            openCount++;
        } else if (current == closingChar && openCount > 0) {
            openCount--;
            if (openCount == 0) {
                return false;
            }
        }
        return true;
    }
}