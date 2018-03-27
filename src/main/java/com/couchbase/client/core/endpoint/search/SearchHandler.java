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
package com.couchbase.client.core.endpoint.search;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.search.GetSearchIndexRequest;
import com.couchbase.client.core.message.search.GetSearchIndexResponse;
import com.couchbase.client.core.message.search.PingRequest;
import com.couchbase.client.core.message.search.PingResponse;
import com.couchbase.client.core.message.search.UpsertSearchIndexRequest;
import com.couchbase.client.core.message.search.UpsertSearchIndexResponse;
import com.couchbase.client.core.message.search.RemoveSearchIndexRequest;
import com.couchbase.client.core.message.search.RemoveSearchIndexResponse;
import com.couchbase.client.core.message.search.SearchQueryRequest;
import com.couchbase.client.core.message.search.SearchQueryResponse;
import com.couchbase.client.core.message.search.SearchRequest;
import com.couchbase.client.core.service.ServiceType;
import com.lmax.disruptor.EventSink;
import com.lmax.disruptor.RingBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

import java.util.Queue;

/**
 * The {@link SearchHandler} is responsible for encoding {@link SearchRequest}s into lower level
 * {@link HttpRequest}s as well as decoding {@link HttpObject}s into
 * {@link CouchbaseResponse}s.
 *
 * @author Sergey Avseyev
 * @since 1.2
 */
public class SearchHandler extends AbstractGenericHandler<HttpObject, HttpRequest, SearchRequest> {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(SearchHandler.class);
    /**
     * Contains the current pending response header if set.
     */
    private HttpResponse responseHeader;

    /**
     * Contains the accumulating buffer for the response content.
     */
    private ByteBuf responseContent;

    public SearchHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer, boolean isTransient,
                         final boolean pipeline) {
        super(endpoint, responseBuffer, isTransient, pipeline);
    }

    /**
     * Creates a new {@link SearchHandler} with a custom queue for requests (suitable for tests).
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     * @param queue the queue which holds all outstanding open requests.
     */
    SearchHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer, Queue<SearchRequest> queue,
                  boolean isTransient, final boolean pipeline) {
        super(endpoint, responseBuffer, queue, isTransient, pipeline);
    }

    @Override
    protected HttpRequest encodeRequest(ChannelHandlerContext ctx, SearchRequest msg) throws Exception {
        HttpMethod httpMethod = HttpMethod.GET;
        if (msg instanceof UpsertSearchIndexRequest) {
            httpMethod = HttpMethod.PUT;
        } else if (msg instanceof RemoveSearchIndexRequest) {
            httpMethod = HttpMethod.DELETE;
        } else if (msg instanceof SearchQueryRequest) {
            httpMethod = HttpMethod.POST;
        }

        ByteBuf content;
        if (msg instanceof UpsertSearchIndexRequest) {
            content = Unpooled.copiedBuffer(((UpsertSearchIndexRequest) msg).payload(), CharsetUtil.UTF_8);
        } else if (msg instanceof SearchQueryRequest) {
            content = Unpooled.copiedBuffer(((SearchQueryRequest) msg).payload(), CharsetUtil.UTF_8);
        } else {
            content = Unpooled.EMPTY_BUFFER;
        }

        FullHttpRequest request;

        if (msg instanceof KeepAliveRequest || msg instanceof PingRequest) {
            request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, msg.path(), content);
            request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
            request.headers().set(HttpHeaders.Names.HOST, remoteHttpHost(ctx));
        } else {
            request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, msg.path(), content);
            request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
            if (msg instanceof UpsertSearchIndexRequest || msg instanceof SearchQueryRequest) {
                request.headers().set(HttpHeaders.Names.ACCEPT, "*/*");
                request.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json");
            }
            request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes());
            request.headers().set(HttpHeaders.Names.HOST, remoteHttpHost(ctx));
            addHttpBasicAuth(ctx, request, msg.username(), msg.password());
        }

        return request;
    }

    @Override
    protected CouchbaseResponse decodeResponse(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        SearchRequest request = currentRequest();
        CouchbaseResponse response = null;

        if (msg instanceof HttpResponse) {
            responseHeader = (HttpResponse) msg;

            if (responseContent != null) {
                responseContent.clear();
            } else {
                responseContent = ctx.alloc().buffer();
            }
        }


        if (msg instanceof HttpContent) {
            responseContent.writeBytes(((HttpContent) msg).content());
        }

        if (currentRequest() instanceof KeepAliveRequest) {
            if (msg instanceof LastHttpContent) {
                response = new KeepAliveResponse(ResponseStatusConverter.fromHttp(responseHeader.getStatus().code()), currentRequest());
                responseContent.clear();
                responseContent.discardReadBytes();
                finishedDecoding();
            }
        } else if (currentRequest() instanceof PingRequest) {
            if (msg instanceof LastHttpContent) {
                response = new PingResponse(ResponseStatusConverter.fromHttp(responseHeader.getStatus().code()), currentRequest());
                responseContent.clear();
                responseContent.discardReadBytes();
                finishedDecoding();
            }
        } else if (msg instanceof LastHttpContent) {
            ResponseStatus status = ResponseStatusConverter.fromHttp(responseHeader.getStatus().code());
            String body = responseContent.readableBytes() > 0
                    ? responseContent.toString(CHARSET) : responseHeader.getStatus().reasonPhrase();

            if (request instanceof UpsertSearchIndexRequest) {
                response = new UpsertSearchIndexResponse(body, status);
            } else if (request instanceof GetSearchIndexRequest) {
                response = new GetSearchIndexResponse(body, status);
            } else if (request instanceof RemoveSearchIndexRequest) {
                response = new RemoveSearchIndexResponse(body, status);
            } else if (request instanceof SearchQueryRequest) {
                //TODO if more parsing is implemented, add a RawSearchQueryRequest similar to what was done in JVMCBC-357
                response = new SearchQueryResponse(body, status);
            }

            finishedDecoding();
        }
        return response;
    }

    @Override
    protected void finishedDecoding() {
        releaseResponseContent();
        super.finishedDecoding();
    }

    private void releaseResponseContent() {
        if (responseContent != null && responseContent.refCnt() > 0) {
            responseContent.release();
        }
        responseContent = null;
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        releaseResponseContent();
        super.handlerRemoved(ctx);
    }

    @Override
    protected ServiceType serviceType() {
        return ServiceType.SEARCH;
    }

    @Override
    protected CouchbaseRequest createKeepAliveRequest() {
        return new KeepAliveRequest();
    }

    protected static class KeepAliveRequest extends AbstractCouchbaseRequest implements SearchRequest {
        protected KeepAliveRequest() {
            super(null, null);
        }

        @Override
        public String path() {
            return "/api/ping";
        }
    }

    protected static class KeepAliveResponse extends AbstractCouchbaseResponse {
        protected KeepAliveResponse(ResponseStatus status, CouchbaseRequest request) {
            super(status, request);
        }
    }
}
