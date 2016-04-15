/*
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

import io.netty.buffer.ByteBufProcessor;

/**
 * A {@link io.netty.buffer.ByteBufProcessor} to find the closing position of a JSON string.
 * Applying this to a buffer will output the position of the closing of the string, relative to that buffer's
 * readerIndex, or -1 if the end of the section couldn't be found.
 *
 * It'll take into account the string's opening quote (which is expected to be after the current readerIndex),
 * and ignore escaped quotes inside the string.
 *
 * It is invoked on a {@link io.netty.buffer.ByteBuf} by calling
 * {@link io.netty.buffer.ByteBuf#forEachByte(io.netty.buffer.ByteBufProcessor)} methods.
 * *
 * @author Simon Baslé
 * @since 1.1
 */
public class StringClosingPositionBufProcessor implements ByteBufProcessor {

    private boolean inString = false;
    private byte lastByte = 0;
    private byte beforeLastByte = 0;

    @Override
    public boolean process(byte value) throws Exception {
        boolean done;
        if (!inString && value == '"') {
            inString = true;
            done = false;
        } else if (inString && value == '"') {
            boolean escaped = lastByte == '\\' && beforeLastByte != '\\';
            if (escaped) {
                done = false;
            } else {
                inString = false;
                done = true;
            }
        } else {
            done = false;
        }

        beforeLastByte = lastByte;
        lastByte = value;
        return !done;
    }
}
