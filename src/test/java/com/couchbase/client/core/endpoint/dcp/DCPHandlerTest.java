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

package com.couchbase.client.core.endpoint.dcp;

import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.StreamRequestRequest;
import com.couchbase.client.core.util.CollectingResponseEventSink;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


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
        AbstractEndpoint endpoint = mock(AbstractEndpoint.class);
        when(endpoint.environment()).thenReturn(DefaultCoreEnvironment.builder().dcpEnabled(true).build());
        channel = new EmbeddedChannel(new DCPHandler(endpoint, eventSink, requestQueue, true, true));
    }

    @Test
    public void shouldEncodeStreamRequestRequest() {
        StreamRequestRequest request = new StreamRequestRequest(
                (short) 1234,
                (long) 5678,
                (long) 9012,
                (long) 3456,
                (long) 7890,
                (long) 12345,
                null, null, null
        );

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals(DCPHandler.OP_STREAM_REQUEST, outbound.getOpcode());
        assertEquals(0, outbound.getKeyLength());
        assertEquals(48, outbound.getTotalBodyLength());
        assertEquals(1234, outbound.getReserved());
        assertEquals(48, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());        // flags
        assertEquals(0, outbound.getExtras().readInt());        // reserved
        assertEquals(9012, outbound.getExtras().readLong());    // start sequence number
        assertEquals(3456, outbound.getExtras().readLong());    // end sequence number
        assertEquals(5678, outbound.getExtras().readLong());    // vbucket UUID
        assertEquals(7890, outbound.getExtras().readLong());    // snapshot start sequence number
        assertEquals(12345, outbound.getExtras().readLong());   // snapshot end sequence number

        ReferenceCountUtil.releaseLater(outbound);
    }

}
