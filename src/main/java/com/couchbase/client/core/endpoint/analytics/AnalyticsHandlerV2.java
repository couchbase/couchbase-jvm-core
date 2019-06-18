/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint.analytics;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.analytics.parser.YasjlAnalyticsDeferredResponseParser;
import com.couchbase.client.core.endpoint.analytics.parser.YasjlAnalyticsResponseParser;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.KeepAlive;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.analytics.AnalyticsQueryResultRequest;
import com.couchbase.client.core.message.analytics.AnalyticsQueryStatusRequest;
import com.couchbase.client.core.message.analytics.AnalyticsRequest;
import com.couchbase.client.core.message.analytics.GenericAnalyticsRequest;
import com.couchbase.client.core.message.analytics.RawAnalyticsRequest;
import com.couchbase.client.core.message.analytics.RawAnalyticsResponse;
import com.couchbase.client.core.message.analytics.PingRequest;
import com.couchbase.client.core.message.analytics.PingResponse;
import com.couchbase.client.core.service.ServiceType;
import com.lmax.disruptor.RingBuffer;
import io.netty.buffer.ByteBuf;
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
import java.util.Queue;

/**
 * The {@link AnalyticsHandlerV2} is responsible for encoding {@link AnalyticsRequest}s into lower level
 * {@link HttpRequest}s as well as decoding {@link HttpObject}s into
 * {@link CouchbaseResponse}s. The query response parser uses {@link YasjlAnalyticsResponseParser}
 *
 * @author Subhashni Balakrishnan
 */
public class AnalyticsHandlerV2 extends AbstractGenericHandler<HttpObject, HttpRequest, AnalyticsRequest>{

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(AnalyticsHandlerV2.class);

    /**
     * Contains the current pending response header if set.
     */
    private HttpResponse responseHeader;

    /**
     * Contains the accumulating buffer for the response content.
     */
    private ByteBuf responseContent;

    final private YasjlAnalyticsResponseParser parser;

    final private YasjlAnalyticsDeferredResponseParser deferredResponseParser;

    /**
     * Creates a new {@link AnalyticsHandlerV2} with the default queue for requests.
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     */
    public AnalyticsHandlerV2(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer, boolean isTransient,
                          final boolean pipeline) {
        super(endpoint, responseBuffer, isTransient, pipeline);
        deferredResponseParser = new YasjlAnalyticsDeferredResponseParser(env().scheduler(), env().autoreleaseAfter(), endpoint.environment());
        parser = new YasjlAnalyticsResponseParser(env().scheduler(), env().autoreleaseAfter(), endpoint.environment());
    }


    @Override
    protected HttpRequest encodeRequest(final ChannelHandlerContext ctx, final AnalyticsRequest msg) throws Exception {
        FullHttpRequest request;

        if (msg instanceof GenericAnalyticsRequest) {
            GenericAnalyticsRequest queryRequest = (GenericAnalyticsRequest) msg;
            request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/analytics/service");
            if (queryRequest.isJsonFormat()) {
                request.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json");
            }
            if (queryRequest.priority() != GenericAnalyticsRequest.NO_PRIORITY) {
                request.headers().set("Analytics-Priority", queryRequest.priority());
            }
            ByteBuf query = ctx.alloc().buffer(((GenericAnalyticsRequest) msg).query().length());
            query.writeBytes(((GenericAnalyticsRequest) msg).query().getBytes(CHARSET));
            request.headers().add(HttpHeaders.Names.CONTENT_LENGTH, query.readableBytes());
            request.content().writeBytes(query);
            query.release();
        } else if (msg instanceof com.couchbase.client.core.message.analytics.PingRequest || msg instanceof AnalyticsHandler.KeepAliveRequest
            || msg instanceof AnalyticsQueryResultRequest || msg instanceof AnalyticsQueryStatusRequest ) {
            request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, msg.path());
        } else {
            throw new IllegalArgumentException("Unknown incoming AnalyticsRequest type "
                    + msg.getClass());
        }

        // Add common headers across requests
        request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
        request.headers().set(HttpHeaders.Names.HOST, remoteHttpHost(ctx));
        addHttpBasicAuth(ctx, request, msg.username(), msg.password());

        return request;
    }

    @Override
    protected CouchbaseResponse decodeResponse(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
        CouchbaseResponse response = null;

        if (msg instanceof HttpResponse) {
            responseHeader = (HttpResponse) msg;
            if (responseContent != null) {
                responseContent.clear();
            } else {
                responseContent = ctx.alloc().buffer();
            }
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
        } else if (msg instanceof HttpContent && currentRequest() instanceof AnalyticsQueryResultRequest) {
            responseContent.writeBytes(((HttpContent) msg).content());
            boolean lastChunk = msg instanceof LastHttpContent;

            //initialize parser for current response
            if (!deferredResponseParser.isInitialized()) {
                deferredResponseParser.initialize(responseContent, ResponseStatusConverter.fromHttp(responseHeader.getStatus().code()), currentRequest());
            }

            response = deferredResponseParser.parse();
            if (lastChunk) {
                deferredResponseParser.finishParsingAndReset();
                finishedDecoding();
            }
        } else if (msg instanceof HttpContent) {
            responseContent.writeBytes(((HttpContent) msg).content());
            boolean lastChunk = msg instanceof LastHttpContent;

            //initialize parser for current response
            if (!parser.isInitialized()) {
                parser.initialize(responseContent, ResponseStatusConverter.fromHttp(responseHeader.getStatus().code()), currentRequest());
            }

            //important to place the RawAnalyticsRequest test before, as it extends GenericAnalyticsRequest
            if (currentRequest() instanceof RawAnalyticsRequest) {
                response = handleRawAnalyticsResponse(lastChunk, ctx);
            } else if (currentRequest() instanceof GenericAnalyticsRequest || currentRequest() instanceof AnalyticsQueryStatusRequest) {
                response = parser.parse();
                if (lastChunk) {
                    parser.finishParsingAndReset();
                    finishedDecoding();
                }
            }
        }

        return response;
    }

    private RawAnalyticsResponse handleRawAnalyticsResponse(boolean lastChunk, ChannelHandlerContext ctx) {
        if (!lastChunk) {
            return null;
        }
        ResponseStatus status = ResponseStatusConverter.fromHttp(responseHeader.getStatus().code());
        ByteBuf responseCopy = ctx.alloc().buffer(responseContent.readableBytes(), responseContent.readableBytes());
        responseCopy.writeBytes(responseContent);

        return new RawAnalyticsResponse(status, currentRequest(), responseCopy,
                responseHeader.getStatus().code(),
                responseHeader.getStatus().reasonPhrase());
    }


    @Override
    protected void finishedDecoding() {
        releaseResponseContent();
        super.finishedDecoding();
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        releaseResponseContent();
        super.handlerRemoved(ctx);
    }

    private void releaseResponseContent() {
        if (responseContent != null && responseContent.refCnt() > 0) {
            responseContent.release();
        }
        responseContent = null;
    }

    @Override
    protected CouchbaseRequest createKeepAliveRequest() {
        return new KeepAliveRequest();
    }

    protected static class KeepAliveRequest extends AbstractCouchbaseRequest implements AnalyticsRequest, KeepAlive {
        protected KeepAliveRequest() {
            super(null, null);
        }

        @Override
        public String path() {
            return "/admin/ping";
        }
    }

    protected static class KeepAliveResponse extends AbstractCouchbaseResponse {
        protected KeepAliveResponse(ResponseStatus status, CouchbaseRequest request) {
            super(status, request);
        }
    }

    @Override
    protected ServiceType serviceType() {
        return ServiceType.ANALYTICS;
    }

    @InterfaceAudience.Private
    public ByteBuf getResponseContent() {
        return responseContent;
    }
}