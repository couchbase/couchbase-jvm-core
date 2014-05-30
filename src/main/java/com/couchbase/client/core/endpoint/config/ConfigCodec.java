package com.couchbase.client.core.endpoint.config;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.BucketConfigResponse;
import com.couchbase.client.core.message.config.BucketStreamingRequest;
import com.couchbase.client.core.message.config.BucketStreamingResponse;
import com.couchbase.client.core.message.config.ConfigRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import rx.subjects.PublishSubject;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class ConfigCodec extends MessageToMessageCodec<HttpObject, ConfigRequest>  {

    /**
     * The Queue which holds the request types so that proper decoding can happen async.
     */
    private final Queue<ConfigRequest> queue;

    private ConfigRequest currentRequest;
    private StringBuilder currentConfig;
    private PublishSubject<String> configStream;

    /**
     * Creates a new {@link ConfigCodec} with the default dequeue.
     */
    public ConfigCodec() {
        this(new ArrayDeque<ConfigRequest>());
    }

    /**
     * Creates a new {@link ConfigCodec} with a custom dequeue.
     *
     * @param queue a custom queue to test encoding/decoding.
     */
    public ConfigCodec(final Queue<ConfigRequest> queue) {
        this.queue = queue;
        this.currentConfig = new StringBuilder();
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ConfigRequest msg, List<Object> out) throws Exception {
        HttpRequest request;
        if (msg instanceof BucketConfigRequest) {
            request = handleBucketConfigRequest(ctx, (BucketConfigRequest) msg);
        } else if (msg instanceof BucketStreamingRequest) {
            request = handleBucketStreamingRequest(ctx, (BucketStreamingRequest) msg);
        } else {
            throw new IllegalArgumentException("Unknown Message to encode: " + msg);
        }

        out.add(request);
        queue.offer(msg);
    }

    private HttpRequest handleBucketStreamingRequest(ChannelHandlerContext ctx, BucketStreamingRequest msg) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, msg.path());
    }

    private HttpRequest handleBucketConfigRequest(ChannelHandlerContext ctx, BucketConfigRequest msg) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, msg.path());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
        if (currentRequest == null) {
            currentRequest = queue.poll();
        }

        if (currentRequest instanceof BucketConfigRequest) {
            handleBucketConfigResponse(msg, out);
        } else if (currentRequest instanceof BucketStreamingRequest) {
            handleBucketStreamingResponse(msg, out);
        }
    }

    private void handleBucketStreamingResponse(HttpObject msg, List<Object> out) {
        if (msg instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) msg;
            if (response.getStatus().code() == 200) {
                configStream = PublishSubject.create();
                out.add(new BucketStreamingResponse(configStream, ResponseStatus.SUCCESS, null));
            }
        }
        if (msg instanceof HttpContent) {
            currentConfig.append(((HttpContent) msg).content().toString(CharsetUtil.UTF_8));
            if (currentConfig.indexOf("\n\n\n\n") > 0) {
                configStream.onNext(currentConfig.toString().trim());
                currentConfig.setLength(0);
            }
        }
    }

    private void handleBucketConfigResponse(final HttpObject msg, final List<Object> out) {
        if (msg instanceof HttpContent) {
            currentConfig.append(((HttpContent) msg).content().toString(CharsetUtil.UTF_8));

            if (msg instanceof LastHttpContent) {
                out.add(new BucketConfigResponse(currentConfig.toString(), ResponseStatus.SUCCESS));
                currentConfig.setLength(0);
                currentRequest = null;
            }
        }
    }
}
