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

package com.couchbase.client.core.endpoint.kv;

import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheResponse;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Verifies the functionality of the {@link KeyValueAuthHandler}.
 */
public class KeyValueAuthHandlerTest {

    @Test
    public void shouldAlwaysPickPlainIfForced() {
        KeyValueAuthHandler handler = new KeyValueAuthHandler("user", "pass", true);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        // Handler sends "list mechs"
        BinaryMemcacheRequest listMechsRequest = (BinaryMemcacheRequest) channel.readOutbound();
        assertEquals(KeyValueAuthHandler.SASL_LIST_MECHS_OPCODE, listMechsRequest.getOpcode());
        ReferenceCountUtil.release(listMechsRequest);

        // Server returns with a bunch of them
        FullBinaryMemcacheResponse listMechsResponse = new DefaultFullBinaryMemcacheResponse(
                new byte[] {},
                Unpooled.EMPTY_BUFFER,
                Unpooled.copiedBuffer("CRAM-MD5 PLAIN", CharsetUtil.UTF_8)
        );
        listMechsResponse.setOpcode(KeyValueAuthHandler.SASL_LIST_MECHS_OPCODE);
        channel.writeInbound(listMechsResponse);

        // make sure it still picks only PLAIN
        FullBinaryMemcacheRequest initialRequest = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertEquals("PLAIN", new String(initialRequest.getKey()));
        ReferenceCountUtil.release(initialRequest);
    }

    @Test
    public void shouldPickHighestMechIfNotForced() {
        KeyValueAuthHandler handler = new KeyValueAuthHandler("user", "pass", false);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        // Handler sends "list mechs"
        BinaryMemcacheRequest listMechsRequest = (BinaryMemcacheRequest) channel.readOutbound();
        assertEquals(KeyValueAuthHandler.SASL_LIST_MECHS_OPCODE, listMechsRequest.getOpcode());
        ReferenceCountUtil.release(listMechsRequest);

        // Server returns with a bunch of them
        FullBinaryMemcacheResponse listMechsResponse = new DefaultFullBinaryMemcacheResponse(
                new byte[] {},
                Unpooled.EMPTY_BUFFER,
                Unpooled.copiedBuffer("SCRAM-SHA1 CRAM-MD5 PLAIN", CharsetUtil.UTF_8)
        );
        listMechsResponse.setOpcode(KeyValueAuthHandler.SASL_LIST_MECHS_OPCODE);

        channel.writeInbound(listMechsResponse);

        // make sure it still picks only PLAIN
        FullBinaryMemcacheRequest initialRequest = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertEquals("SCRAM-SHA1", new String(initialRequest.getKey()));
        ReferenceCountUtil.release(initialRequest);
    }
}