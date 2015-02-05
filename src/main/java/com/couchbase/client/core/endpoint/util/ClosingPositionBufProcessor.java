/**
 * Copyright (c) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
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
 * in a streaming JSON response.
 *
 * It is invoked on a {@link ByteBuf} by calling {@link ByteBuf#forEachByte(ByteBufProcessor)} methods.
 *
 * @author Simon Baslé
 * @since 1.1.0
 */
public class ClosingPositionBufProcessor implements ByteBufProcessor {

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
     * @param openingChar the opening section character (used to detect a sub-section).
     * @param closingChar the closing section character to search for.
     */
    public ClosingPositionBufProcessor(char openingChar, char closingChar) {
        this.openingChar = openingChar;
        this.closingChar = closingChar;
    }

    @Override
    public boolean process(final byte current) throws Exception {
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