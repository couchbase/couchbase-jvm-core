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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.util.Resources;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link QueryHandlerV2} with V2 parser
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.3
 */
public class QueryHandlerV2Test extends QueryHandlerTest {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(QueryHandlerV2Test.class);

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


    @Test
    public void shouldDiscardReadBytesOnChunkedResponse() throws Throwable {
        String response = Resources.read("chunked.json", this.getClass());
        ArrayList<String> chunks = new ArrayList<String>();
        int i = 0;
        while(i+100 < response.length()) {
            chunks.add(response.substring(i, i+100));
            i = i+100;
        }
        chunks.add(response.substring(i));
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        Object[] httpChunks = new Object[chunks.size() + 1];
        httpChunks[0] = responseHeader;
        for (i = 1; i <= chunks.size(); i++) {
            String chunk = chunks.get(i - 1);
            if (i == chunks.size()) {
                httpChunks[i] = new DefaultLastHttpContent(Unpooled.copiedBuffer(chunk, CharsetUtil.UTF_8));
            } else {
                httpChunks[i] = new DefaultHttpContent(Unpooled.copiedBuffer(chunk, CharsetUtil.UTF_8));
            }
        }

        Subject<CouchbaseResponse, CouchbaseResponse> obs = AsyncSubject.create();
        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        when(requestMock.observable()).thenReturn(obs);
        queue.add(requestMock);
        final CountDownLatch latch1 = new CountDownLatch(chunks.size() + 5 + 4);

        obs.subscribe(new Action1<CouchbaseResponse>() {
                          @Override
                          public void call(CouchbaseResponse couchbaseResponse) {
                              GenericQueryResponse response = (GenericQueryResponse) couchbaseResponse;
                              response.rows().subscribe(new Action1<ByteBuf>() {
                                  @Override
                                  public void call(ByteBuf byteBuf) {
                                      byteBuf.release();
                                      if (((QueryHandlerV2) handler).getResponseContent() != null) {
                                          assertTrue(((QueryHandlerV2) handler).getResponseContent().readerIndex() == 1);
                                      }
                                      latch1.countDown();
                                  }
                              }, new Action1<Throwable>() {
                                  @Override
                                  public void call(Throwable throwable) {
                                  }
                              });
                              response.errors().subscribe(new Action1<ByteBuf>() {
                                  @Override
                                  public void call(ByteBuf byteBuf) {
                                      byteBuf.release();
                                      if (((QueryHandlerV2) handler).getResponseContent() != null) {
                                          assertTrue(((QueryHandlerV2) handler).getResponseContent().readerIndex() == 1);
                                      }
                                      latch1.countDown();
                                  }
                              }, new Action1<Throwable>() {
                                  @Override
                                  public void call(Throwable throwable) {

                                  }
                              });
                              response.info().subscribe(new Action1<ByteBuf>() {
                                  @Override
                                  public void call(ByteBuf byteBuf) {
                                      byteBuf.release();
                                  }
                              }, new Action1<Throwable>() {
                                  @Override
                                  public void call(Throwable throwable) {

                                  }
                              });
                              response.signature().subscribe(new Action1<ByteBuf>() {
                                  @Override
                                  public void call(ByteBuf byteBuf) {
                                      byteBuf.release();
                                  }
                              }, new Action1<Throwable>() {
                                  @Override
                                  public void call(Throwable throwable) {

                                  }
                              });

                          }
                      }
        );

        Observable.from(httpChunks).zipWith(Observable.interval(1, TimeUnit.SECONDS).takeWhile(new Func1<Long, Boolean>() {
                @Override
                public Boolean call(Long aLong) {
                    return latch1.getCount() > 0;
                }
            }),
                new Func2<Object, Long, Object>() {
                    @Override
                    public Object call(Object o, Long aLong) {

                        return channel.writeInbound(o);
                    }
                }).subscribe(new Action1<Object>() {

            @Override
            public void call(Object s) {
                latch1.countDown();
            }

        });
        latch1.await();
    }

}
