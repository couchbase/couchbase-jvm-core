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
package com.couchbase.client.core.endpoint.config;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.BucketConfigResponse;
import com.couchbase.client.core.message.config.BucketStreamingRequest;
import com.couchbase.client.core.message.config.BucketStreamingResponse;
import com.couchbase.client.core.message.config.BucketsConfigRequest;
import com.couchbase.client.core.message.config.BucketsConfigResponse;
import com.couchbase.client.core.message.config.ClusterConfigRequest;
import com.couchbase.client.core.message.config.ClusterConfigResponse;
import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.message.config.FlushRequest;
import com.couchbase.client.core.message.config.FlushResponse;
import com.couchbase.client.core.message.config.GetDesignDocumentsRequest;
import com.couchbase.client.core.message.config.GetDesignDocumentsResponse;
import com.couchbase.client.core.message.config.GetUsersRequest;
import com.couchbase.client.core.message.config.GetUsersResponse;
import com.couchbase.client.core.message.config.InsertBucketRequest;
import com.couchbase.client.core.message.config.InsertBucketResponse;
import com.couchbase.client.core.message.config.RemoveBucketRequest;
import com.couchbase.client.core.message.config.RemoveBucketResponse;
import com.couchbase.client.core.message.config.RemoveUserRequest;
import com.couchbase.client.core.message.config.RemoveUserResponse;
import com.couchbase.client.core.message.config.RestApiRequest;
import com.couchbase.client.core.message.config.RestApiResponse;
import com.couchbase.client.core.message.config.UpdateBucketRequest;
import com.couchbase.client.core.message.config.UpdateBucketResponse;
import com.couchbase.client.core.message.config.UpsertUserRequest;
import com.couchbase.client.core.message.config.UpsertUserResponse;
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
import rx.Observable;
import rx.subjects.BehaviorSubject;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;

import static com.couchbase.client.core.logging.RedactableArgument.meta;
import static com.couchbase.client.core.logging.RedactableArgument.system;


/**
 * The {@link ConfigHandler} is responsible for encoding {@link ConfigRequest}s into lower level
 * {@link HttpRequest}s as well as decoding {@link HttpObject}s into
 * {@link CouchbaseResponse}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ConfigHandler extends AbstractGenericHandler<HttpObject, HttpRequest, ConfigRequest> {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ConfigHandler.class);

    /**
     * Contains the current pending response header if set.
     */
    private HttpResponse responseHeader;

    /**
     * Contains the accumulating buffer for the response content.
     */
    private ByteBuf responseContent;

    /**
     * Represents a observable that sends config chunks if instructed.
     */
    private BehaviorSubject<String> streamingConfigObservable;

    /**
     * Creates a new {@link ConfigHandler} with the default queue for requests.
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     */
    public ConfigHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer, boolean isTransient,
                         final boolean pipeline) {
        super(endpoint, responseBuffer, isTransient, pipeline);
    }

    /**
     * Creates a new {@link ConfigHandler} with a custom queue for requests (suitable for tests).
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     * @param queue the queue which holds all outstanding open requests.
     */
    ConfigHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer, Queue<ConfigRequest> queue,
                  boolean isTransient, final boolean pipeline) {
        super(endpoint, responseBuffer, queue, isTransient, pipeline);
    }

    @Override
    protected HttpRequest encodeRequest(final ChannelHandlerContext ctx, final ConfigRequest msg) throws Exception {
        if (msg instanceof RestApiRequest) {
            return encodeRestApiRequest(ctx, (RestApiRequest) msg);
        }
        HttpMethod httpMethod = HttpMethod.GET;
        if (msg instanceof FlushRequest || msg instanceof InsertBucketRequest
                || msg instanceof UpdateBucketRequest) {
            httpMethod = HttpMethod.POST;
        } else if (msg instanceof UpsertUserRequest) {
          httpMethod = HttpMethod.PUT;
        } else if (msg instanceof RemoveBucketRequest || msg instanceof RemoveUserRequest) {
            httpMethod = HttpMethod.DELETE;
        }

        ByteBuf content;
        if (msg instanceof InsertBucketRequest) {
            content = Unpooled.copiedBuffer(((InsertBucketRequest) msg).payload(), CharsetUtil.UTF_8);
        } else if (msg instanceof UpdateBucketRequest) {
            content = Unpooled.copiedBuffer(((UpdateBucketRequest) msg).payload(), CharsetUtil.UTF_8);
        } else if (msg instanceof UpsertUserRequest) {
            content = Unpooled.copiedBuffer(((UpsertUserRequest) msg).payload(), CharsetUtil.UTF_8);
        } else {
            content = Unpooled.EMPTY_BUFFER;
        }

        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, msg.path(), content);
        request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
        if (msg instanceof InsertBucketRequest || msg instanceof UpdateBucketRequest || msg instanceof UpsertUserRequest) {
            request.headers().set(HttpHeaders.Names.ACCEPT, "*/*");
            request.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/x-www-form-urlencoded");
        }
        request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes());
        request.headers().set(HttpHeaders.Names.HOST, remoteHttpHost(ctx));

        addHttpBasicAuth(ctx, request, msg.username(), msg.password());
        return request;
    }

    private HttpRequest encodeRestApiRequest(ChannelHandlerContext ctx, RestApiRequest msg) {
        HttpMethod httpMethod = msg.method();
        ByteBuf content = Unpooled.copiedBuffer(msg.body(), CharsetUtil.UTF_8);
        String path = msg.pathWithParameters();

        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, path, content);
        //these headers COULD be overridden
        request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
        request.headers().set(HttpHeaders.Names.HOST, remoteHttpHost(ctx));

        for (Map.Entry<String, Object> header : msg.headers().entrySet()) {
            request.headers().set(header.getKey(), header.getValue());
        }

        //these headers should always be computed from the msg
        request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes());

        addHttpBasicAuth(ctx, request, msg.username(), msg.password());
        return request;
    }

    @Override
    protected CouchbaseResponse decodeResponse(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
        ConfigRequest request = currentRequest();
        CouchbaseResponse response = null;

        if (msg instanceof HttpResponse) {
            responseHeader = (HttpResponse) msg;

            if (request instanceof BucketStreamingRequest) {
                response = handleBucketStreamingResponse(ctx, responseHeader);
            }

            if (responseContent != null) {
                responseContent.clear();
            } else {
                responseContent = ctx.alloc().buffer();
            }
        }

        if (msg instanceof HttpContent) {
            responseContent.writeBytes(((HttpContent) msg).content());
            if (streamingConfigObservable != null) {
                maybePushConfigChunk();
            }
        }

        if (msg instanceof LastHttpContent) {
            if (request instanceof BucketStreamingRequest) {
                if (streamingConfigObservable != null) {
                    streamingConfigObservable.onCompleted();
                    streamingConfigObservable = null;
                }
                finishedDecoding();
                return null;
            }

            ResponseStatus status = ResponseStatusConverter.fromHttp(responseHeader.getStatus().code());
            String body = responseContent.readableBytes() > 0
                ? responseContent.toString(CHARSET) : responseHeader.getStatus().reasonPhrase();

            if (request instanceof BucketConfigRequest) {
                response = new BucketConfigResponse(body, status);
            } else if (request instanceof ClusterConfigRequest) {
                response = new ClusterConfigResponse(body, status);
            } else if (request instanceof BucketsConfigRequest) {
                response = new BucketsConfigResponse(body, status);
            } else if (request instanceof GetDesignDocumentsRequest) {
                response = new GetDesignDocumentsResponse(body, status, request);
            } else if (request instanceof RemoveBucketRequest) {
                response = new RemoveBucketResponse(status);
            } else if (request instanceof InsertBucketRequest) {
                response = new InsertBucketResponse(body, status);
            } else if (request instanceof UpdateBucketRequest) {
                response = new UpdateBucketResponse(body, status);
            } else if (request instanceof FlushRequest) {
                boolean done = responseHeader.getStatus().code() != 201;
                response = new FlushResponse(done, body, status);
            } else if (request instanceof GetUsersRequest) {
                response = new GetUsersResponse(body, status, request);
            } else if (request instanceof UpsertUserRequest) {
                response = new UpsertUserResponse(body, status);
            }  else if (request instanceof RemoveUserRequest) {
                response = new RemoveUserResponse(status);
            } else if (request instanceof RestApiRequest) {
                response = new RestApiResponse((RestApiRequest) request, responseHeader.getStatus(),
                        responseHeader.headers(), body);
            }

            finishedDecoding();
        }

        return response;
    }

    /**
     * Decodes a {@link BucketStreamingResponse}.
     *
     * @param ctx the handler context.
     * @param header the received header.
     * @return a initialized {@link CouchbaseResponse}.
     */
    private CouchbaseResponse handleBucketStreamingResponse(final ChannelHandlerContext ctx,
        final HttpResponse header) {
        SocketAddress addr = ctx.channel().remoteAddress();
        String host = addr instanceof InetSocketAddress ? ((InetSocketAddress) addr).getAddress().getHostAddress()
            : addr.toString();
        ResponseStatus status = ResponseStatusConverter.fromHttp(header.getStatus().code());

        Observable<String> scheduledObservable = null;
        if (status.isSuccess()) {
            streamingConfigObservable = BehaviorSubject.create();
            scheduledObservable = streamingConfigObservable.onBackpressureBuffer().observeOn(env().scheduler());
        }
        return new BucketStreamingResponse(
            scheduledObservable,
            host,
            status,
            currentRequest()
        );
    }

    /**
     * Push a config chunk into the streaming observable.
     */
    private void maybePushConfigChunk() {
        String currentChunk = responseContent.toString(CHARSET);

        int separatorIndex = currentChunk.indexOf("\n\n\n\n");
        if (separatorIndex > 0) {
            String content = currentChunk.substring(0, separatorIndex);
            streamingConfigObservable.onNext(content.trim());
            responseContent.clear();
            responseContent.writeBytes(currentChunk.substring(separatorIndex + 4).getBytes(CHARSET));
        }
    }

    /**
     * If it is still present and open, release the content buffer. Also set it
     * to null so that next decoding can take a new buffer from the pool.
     */
    private void releaseResponseContent() {
        if (responseContent != null) {
            if (responseContent.refCnt() > 0) {
                responseContent.release();
            }
            responseContent = null;
        }
    }

    @Override
    protected void finishedDecoding() {
        super.finishedDecoding();
        releaseResponseContent();
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        if (streamingConfigObservable != null) {
            try {
                streamingConfigObservable.onCompleted();
            } catch (RejectedExecutionException ex) {
                // this can happen during shutdown, so log it but don't let it
                // bubble up the event loop.
                LOGGER.info("{}Could not complete config stream, scheduler shut "
                    + "down already.", logIdent(ctx, endpoint()));
            }
        }
        super.handlerRemoved(ctx);
        releaseResponseContent();
    }

    @Override
    protected ServiceType serviceType() {
        return ServiceType.CONFIG;
    }
}
