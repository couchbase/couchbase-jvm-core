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