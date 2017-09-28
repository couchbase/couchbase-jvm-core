/**
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
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the functionality of the {@link KeyValueSelectBucketHandler}.
 */
public class KeyValueSelectBucketHandlerTest {


    /**
     * We are not importing the opcode from the handler to avoid tests passing
     * if it should change not on purpose.
     */
    private static final byte OPCODE = (byte) 0x89;

    @Test
    public void selectBucketShouldCompleteConnectFuture() throws Exception {
        // Register the Handler
        KeyValueSelectBucketHandler handler = new KeyValueSelectBucketHandler("bucket", true);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        ChannelFuture connectFuture = channel.connect(new InetSocketAddress("127.0.0.1", 11210));

        // Make sure the handler sends the select bucket command the right way
        BinaryMemcacheRequest request = (BinaryMemcacheRequest) channel.readOutbound();
        assertEquals(OPCODE, request.getOpcode());
        assertEquals("bucket".length(), request.getKeyLength());
        assertEquals("bucket".length(), request.getTotalBodyLength());
        assertEquals("bucket", new String(request.getKey(), CharsetUtil.UTF_8));

        // now fake a "not found" response
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(
                new byte[] {}, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER
        );
        response.setStatus((byte) 0x00); // not found error
        channel.writeInbound(response);

        assertTrue(connectFuture.isDone());
        assertTrue(connectFuture.isSuccess());
        assertTrue(channel.pipeline().toMap().isEmpty()); // handler removes itself
    }

    @Test
    public void selectBucketShouldFailConnectFuture() throws Exception {
        // Register the Handler
        KeyValueSelectBucketHandler handler = new KeyValueSelectBucketHandler("bucket", true);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        ChannelFuture connectFuture = channel.connect(new InetSocketAddress("127.0.0.1", 11210));

        // Make sure the handler sends the select bucket command the right way
        BinaryMemcacheRequest request = (BinaryMemcacheRequest) channel.readOutbound();
        assertEquals(OPCODE, request.getOpcode());
        assertEquals("bucket".length(), request.getKeyLength());
        assertEquals("bucket".length(), request.getTotalBodyLength());
        assertEquals("bucket", new String(request.getKey(), CharsetUtil.UTF_8));

        // now fake a "not found" response
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(
                new byte[] {}, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER
        );
        response.setStatus((byte) 0x01); // not found error
        channel.writeInbound(response);

        assertTrue(connectFuture.isDone());
        assertFalse(connectFuture.isSuccess());
        assertTrue(connectFuture.cause() instanceof AuthenticationException);
        assertEquals("Bucket not found on Select Bucket command", connectFuture.cause().getMessage());
    }
}
