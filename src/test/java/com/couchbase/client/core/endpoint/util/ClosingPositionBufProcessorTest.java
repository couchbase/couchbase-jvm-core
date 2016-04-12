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

public class ClosingPositionBufProcessorTest {

    @Test
    public void shouldFindClosingInSimpleSection() {
        ByteBuf source = Unpooled.copiedBuffer("{ this is simple }", CharsetUtil.UTF_8);
        int closingPos = source.forEachByte(new ClosingPositionBufProcessor('{', '}'));
        assertEquals(17, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void shouldNotFindClosingInBrokenSection() {
        ByteBuf source = Unpooled.copiedBuffer("{ this is simple", CharsetUtil.UTF_8);
        int closingPos = source.forEachByte(new ClosingPositionBufProcessor('{', '}'));
        assertEquals(-1, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void shouldFindClosingInSectionWithSubsection() {
        ByteBuf source = Unpooled.copiedBuffer("{ this is { simple } }", CharsetUtil.UTF_8);
        int closingPos = source.forEachByte(new ClosingPositionBufProcessor('{', '}'));
        assertEquals(21, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void shouldNotFindClosingInBrokenSectionWithCompleteSubSection() {
        ByteBuf source = Unpooled.copiedBuffer("{ this is { complex } oups", CharsetUtil.UTF_8);
        int closingPos = source.forEachByte(new ClosingPositionBufProcessor('{', '}'));
        assertEquals(-1, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void shouldIgnoreJsonStringWithRandomSectionChars() {
        ByteBuf source = Unpooled.copiedBuffer(
                "{ this is \"a string \\\"with escaped quote and sectionChars like } or {{{!\" }", CharsetUtil.UTF_8);

        int closingPos = source.forEachByte(new ClosingPositionBufProcessor('{', '}', true));

        assertEquals(74, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void shouldIgnoreJsonStringWithClosingSectionCharEvenIfStreamInterrupted() {
        ByteBuf source = Unpooled.copiedBuffer(
                "{ this is \"a string \\\"with }", CharsetUtil.UTF_8);

        int closingPos = source.forEachByte(new ClosingPositionBufProcessor('{', '}', true));

        assertEquals(-1, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void shouldSkipStringWithEscapedBackslashJustBeforeClosingQuote() {
        ByteBuf source = Unpooled.copiedBuffer("{\"some\": \"weird }object\\\\\"}", CharsetUtil.UTF_8);
        int closingPos = source.forEachByte(new ClosingPositionBufProcessor('{', '}', true));

        assertEquals(26, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void shouldSkipEmptyString() {
        ByteBuf source = Unpooled.copiedBuffer("{\"some\": \"\"}", CharsetUtil.UTF_8);
        int closingPos = source.forEachByte(new ClosingPositionBufProcessor('{', '}', true));

        assertEquals(11, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test
    public void shouldSkipStringWithEscapedBackslashOnly() {
        ByteBuf source = Unpooled.copiedBuffer("{\"some\": \"\\\\\"}", CharsetUtil.UTF_8);
        int closingPos = source.forEachByte(new ClosingPositionBufProcessor('{', '}', true));

        assertEquals(13, closingPos);
        assertEquals(0, source.readerIndex());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentWhenSymmetricChars() {
        new ClosingPositionBufProcessor('"', '"', true);
    }
}