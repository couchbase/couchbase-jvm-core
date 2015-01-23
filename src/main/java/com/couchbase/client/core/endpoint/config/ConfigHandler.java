/**
 * Copyright (C) 2014 Couchbase, Inc.
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
package com.couchbase.client.core.endpoint.config;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
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
import com.couchbase.client.core.message.config.InsertBucketRequest;
import com.couchbase.client.core.message.config.InsertBucketResponse;
import com.couchbase.client.core.message.config.RemoveBucketRequest;
import com.couchbase.client.core.message.config.RemoveBucketResponse;
import com.couchbase.client.core.message.config.UpdateBucketRequest;
import com.couchbase.client.core.message.config.UpdateBucketResponse;
import com.lmax.disruptor.EventSink;
import com.lmax.disruptor.RingBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.base64.Base64;
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
import java.util.Queue;


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
    public ConfigHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer, boolean isTransient) {
        super(endpoint, responseBuffer, isTransient);
    }

    /**
     * Creates a new {@link ConfigHandler} with a custom queue for requests (suitable for tests).
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     * @param queue the queue which holds all outstanding open requests.
     */
    ConfigHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer, Queue<ConfigRequest> queue, boolean isTransient) {
        super(endpoint, responseBuffer, queue, isTransient);
    }

    @Override
    protected HttpRequest encodeRequest(final ChannelHandlerContext ctx, final ConfigRequest msg) throws Exception {
        HttpMethod httpMethod = HttpMethod.GET;
        if (msg instanceof FlushRequest || msg instanceof InsertBucketRequest || msg instanceof UpdateBucketRequest) {
            httpMethod = HttpMethod.POST;
        } else if (msg instanceof RemoveBucketRequest) {
            httpMethod = HttpMethod.DELETE;
        }

        ByteBuf content;
        if (msg instanceof InsertBucketRequest) {
            content = Unpooled.copiedBuffer(((InsertBucketRequest) msg).payload(), CharsetUtil.UTF_8);
        } else if (msg instanceof UpdateBucketRequest) {
            content = Unpooled.copiedBuffer(((UpdateBucketRequest) msg).payload(), CharsetUtil.UTF_8);
        } else {
            content = Unpooled.EMPTY_BUFFER;
        }

        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, msg.path(), content);
        request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
        if (msg instanceof InsertBucketRequest || msg instanceof UpdateBucketRequest) {
            request.headers().set(HttpHeaders.Names.ACCEPT, "*/*");
            request.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/x-www-form-urlencoded");
        }
        request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes());

        addAuth(ctx, request, msg.bucket(), msg.password());

        return request;
    }

    /**
     * Add basic authentication headers to a {@link HttpRequest}.
     *
     * The given information is Base64 encoded and the authorization header is set appropriately. Since this needs
     * to be done for every request, it is refactored out.
     *
     * @param ctx the handler context.
     * @param request the request where the header should be added.
     * @param user the username for auth.
     * @param password the password for auth.
     */
    private static void addAuth(final ChannelHandlerContext ctx, final HttpRequest request, final String user,
        final String password) {
        final String pw = password == null ? "" : password;

        ByteBuf raw = ctx.alloc().buffer(user.length() + pw.length() + 1);
        raw.writeBytes((user + ":" + pw).getBytes(CHARSET));
        ByteBuf encoded = Base64.encode(raw);
        request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded.toString(CHARSET));
        encoded.release();
        raw.release();
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

            ResponseStatus status = statusFromCode(responseHeader.getStatus().code());
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
        String host = addr instanceof InetSocketAddress ? ((InetSocketAddress) addr).getHostName() : addr.toString();
        ResponseStatus status = statusFromCode(header.getStatus().code());

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
     * Converts a HTTP status code in its appropriate {@link ResponseStatus} representation.
     *
     * @param code the http code.
     * @return the parsed status.
     */
    private static ResponseStatus statusFromCode(int code) {
        ResponseStatus status;
        switch (code) {
            case 200:
            case 201:
            case 202:
                status = ResponseStatus.SUCCESS;
                break;
            case 404:
                status = ResponseStatus.NOT_EXISTS;
                break;
            default:
                status = ResponseStatus.FAILURE;
        }
        return status;
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
            streamingConfigObservable.onCompleted();
        }
        super.handlerRemoved(ctx);
        releaseResponseContent();
    }

}
