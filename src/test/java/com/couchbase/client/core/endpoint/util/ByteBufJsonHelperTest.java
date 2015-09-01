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

public class ByteBufJsonHelperTest {

    @Test
    public void testFindNextChar() throws Exception {
        ByteBuf source = Unpooled.copiedBuffer("ABCDEF", CharsetUtil.UTF_8);

        assertEquals(3, ByteBufJsonHelper.findNextChar(source, 'D'));
        assertEquals(-1, ByteBufJsonHelper.findNextChar(source, 'G'));
        assertEquals(0, source.readerIndex());

    }

    @Test
    public void testFindNextCharNotPrefixedBy() throws Exception {
        ByteBuf source = Unpooled.copiedBuffer("the \\\"end\\\" of a string\"", CharsetUtil.UTF_8);

        assertEquals(5, ByteBufJsonHelper.findNextCharNotPrefixedBy(source, '\"', 'a'));
        assertEquals(23, ByteBufJsonHelper.findNextCharNotPrefixedBy(source, '\"', '\\'));
        assertEquals(-1, ByteBufJsonHelper.findNextCharNotPrefixedBy(source, 'a', ' '));
        assertEquals(-1, ByteBufJsonHelper.findNextCharNotPrefixedBy(source, 'z', ' '));
        assertEquals(0, source.readerIndex());
    }

    /**
     * See JVMCBC-239.
     */
    @Test
    public void testFindNextCharNotPrefixedByAfterReaderIndexMoved() throws Exception {
        //before JVMCBC-239 the fact that the head of the buffer, with skipped data, is larger than its tail would
        //cause an negative search length and the method would return a -1.
        ByteBuf source = Unpooled.copiedBuffer("sectionWithLotsAndLotsOfDataToSkip\": \"123456\\\"78901234567890\"",
                CharsetUtil.UTF_8);
        source.skipBytes(38);

        assertEquals(22, ByteBufJsonHelper.findNextCharNotPrefixedBy(source, '"', '\\'));
    }

    @Test
    public void shouldFindSectionClosingPositionIfAtStartSection() throws Exception {
        ByteBuf source = Unpooled.copiedBuffer("{123}", CharsetUtil.UTF_8);

        assertEquals(4, ByteBufJsonHelper.findSectionClosingPosition(source, '{', '}'));
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void shouldNotFindSectionClosingPositionIfAfterStartSection() throws Exception {
        ByteBuf source = Unpooled.copiedBuffer("123}", CharsetUtil.UTF_8);

        assertEquals(-1, ByteBufJsonHelper.findSectionClosingPosition(source, '{', '}'));
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void shouldFindSectionClosingPositionWithOffset() throws Exception {
        ByteBuf source = Unpooled.copiedBuffer("{{{}{123}", CharsetUtil.UTF_8);

        assertEquals(8, ByteBufJsonHelper.findSectionClosingPosition(source, 4, '{', '}'));
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void shouldFindSectionClosingInRealLifeExample() {
        ByteBuf source = Unpooled.copiedBuffer("rics\":{\"elapsedTime\":\"128.21463ms\",\"executionTime\":\"127.21463ms\",\"resultCount\":1,\"resultSize\":0,\"mutationCount\":0,\"errorCount\":0,\"warningCount\":0}}",
                CharsetUtil.UTF_8);

        assertNotEquals(-1, ByteBufJsonHelper.findSectionClosingPosition(source, '{', '}'));
    }

}