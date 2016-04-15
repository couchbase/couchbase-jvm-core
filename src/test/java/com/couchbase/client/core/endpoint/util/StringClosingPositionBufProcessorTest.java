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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import static org.junit.Assert.*;

public class StringClosingPositionBufProcessorTest {

    @Test
    public void testClosingPosFoundInSimpleString() {
        ByteBuf source = Unpooled.copiedBuffer("\" \"", CharsetUtil.UTF_8);

        int closingPos = source.forEachByte(new StringClosingPositionBufProcessor());

        assertEquals(2, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void testClosingPosFoundInStringWithEscapedContent() {
        ByteBuf source = Unpooled.copiedBuffer(" \"Some string with {\\\"escaped\\\"} strings\" \"otherString\"",
                CharsetUtil.UTF_8);

        int closingPos = source.forEachByte(new StringClosingPositionBufProcessor());

        assertEquals(40, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void testClosingPosNotFoundInPartialStringLeftPart() {
        ByteBuf source = Unpooled.copiedBuffer(" \"\\\"Partial\\\" str",
                CharsetUtil.UTF_8);

        int closingPos = source.forEachByte(new StringClosingPositionBufProcessor());

        assertEquals(-1, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void testClosingPosNotFoundInPartialStringRightPart() {
        ByteBuf source = Unpooled.copiedBuffer("ring\"",
                CharsetUtil.UTF_8);

        int closingPos = source.forEachByte(new StringClosingPositionBufProcessor());

        assertEquals(-1, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void testClosingPosFoundInStringWithEscapedBackslashLast() {
        ByteBuf source = Unpooled.copiedBuffer("\"abc\\\\\"", CharsetUtil.UTF_8);
        int closingPos = source.forEachByte(new StringClosingPositionBufProcessor());

        assertEquals(6, closingPos);
        assertEquals(0, source.readerIndex());
    }
}