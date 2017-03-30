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
package com.couchbase.client.core.endpoint.query;

import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.util.Resources;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.junit.Before;
import org.junit.Test;
import rx.functions.Action0;
import rx.functions.Action1;

import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Verifies the correct functionality of the {@link QueryHandlerV2} with V2 parser
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.3
 */
public class QueryHandlerV2Test extends QueryHandlerTest {
   @Override
   @Before
   @SuppressWarnings("unchecked")
   public void setup() {
       commonSetup();
       handler = new QueryHandlerV2(endpoint, responseRingBuffer, queue, false, false);
       channel = new EmbeddedChannel(handler);
   }

    @Test
    public void shouldDecodeProfileInfo() throws Exception {
        String response = Resources.read("with_timings_profile.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        final AtomicInteger invokeCounter1 = new AtomicInteger();
        latch = new CountDownLatch(1);
        inbound.profileInfo().subscribe(
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        invokeCounter1.incrementAndGet();
                        buf.release();
                    }
                },
                new Action1<Throwable>() {
                    @Override
                    public void call(Throwable err) {
                        latch.countDown();
                    }
                },
                new Action0() {
                    @Override
                    public void call() {
                        latch.countDown();
                    }
                }
        );
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, invokeCounter1.get());
    }
}
