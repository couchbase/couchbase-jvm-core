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

package com.couchbase.client.core.endpoint.dcp;

import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.message.dcp.ConnectionType;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.util.CollectingResponseEventSink;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;


/**
 * Verifies functionality of {@link DCPHandler}.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
public class DCPHandlerTest {
    /**
     * The name of the bucket.
     */
    private static final String BUCKET = "bucket";

    /**
     * The channel in which the handler is tested.
     */
    private EmbeddedChannel channel;

    /**
     * The queue in which requests are pushed to only test decodes in isolation manually.
     */
    private Queue<DCPRequest> requestQueue;

    /**
     * Represents a custom event sink that collects all events pushed into it.
     */
    private CollectingResponseEventSink eventSink;

    @Before
    public void setup() {
        eventSink = new CollectingResponseEventSink();
        requestQueue = new ArrayDeque<DCPRequest>();
        channel = new EmbeddedChannel(new DCPHandler(mock(AbstractEndpoint.class), eventSink, requestQueue, true));
    }

    @Test
    public void shouldEncodeOpenConnectionRequest() {
        String connectionName = "foobar";
        OpenConnectionRequest request = new OpenConnectionRequest(connectionName,
                ConnectionType.CONSUMER, 42, BUCKET, null);
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals(connectionName, outbound.getKey());
        assertEquals(connectionName.length(), outbound.getKeyLength());
        assertEquals(connectionName.length() + 8, outbound.getTotalBodyLength());
        assertEquals(1, outbound.getReserved());
        assertEquals(DCPHandler.OP_OPEN_CONNECTION, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(42, outbound.getExtras().readInt());
        assertEquals(1, outbound.getExtras().readInt());
    }

}
