/*
 * Copyright (c) 2017 Couchbase, Inc.
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

package com.couchbase.client.core.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Verifies the functionality of {@link ResponseStatusDetails}.
 *
 * @author Michael Nitschinger
 * @since 1.4.7
 */
public class ResponseStatusDetailsTest {

    @Test
    public void shouldConvertCorrectDetails() {
        String raw = "{ \"error\" : { \"context\" : \"textual context information\", \"ref\" : \"error reference to be found in the server logs\" }}";
        ByteBuf input = Unpooled.copiedBuffer(raw, CharsetUtil.UTF_8);
        ResponseStatusDetails rsd  = ResponseStatusDetails.convert(input);

        assertEquals("textual context information", rsd.context());
        assertEquals("error reference to be found in the server logs", rsd.reference());
        assertEquals(1, input.refCnt());
    }

    @Test
    public void shouldGracefullyHandleInvalidJSON() {
        String raw = "{ \"err\" : { \"context\" : \"...\", \"ref\" : \"...\" }}";
        ByteBuf input = Unpooled.copiedBuffer(raw, CharsetUtil.UTF_8);
        ResponseStatusDetails rsd  = ResponseStatusDetails.convert(input);
        assertNull(rsd);
    }

    @Test
    public void shouldGracefullyHandleEmptyBuf() {
        ResponseStatusDetails rsd  = ResponseStatusDetails.convert(Unpooled.buffer());
        assertNull(rsd);
    }
}