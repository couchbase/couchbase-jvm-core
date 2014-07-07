package com.couchbase.client.core.endpoint.config;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.BucketConfigResponse;
import com.couchbase.client.core.message.config.BucketStreamingRequest;
import com.couchbase.client.core.message.config.BucketStreamingResponse;
import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.message.config.FlushRequest;
import com.couchbase.client.core.message.config.FlushResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import rx.subjects.PublishSubject;

import java.net.InetSocketAddress;
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
    private int currentStatus;

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
        } else if(msg instanceof FlushRequest) {
            request = handleFlushRequest(ctx, (FlushRequest) msg);
        } else {
            throw new IllegalArgumentException("Unknown Message to encode: " + msg);
        }


        ByteBuf raw = Unpooled.copiedBuffer(msg.bucket() + ":" + msg.password(), CharsetUtil.UTF_8);
        ByteBuf encoded = Base64.encode(raw);
        request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded.toString(CharsetUtil.UTF_8));
        encoded.release();
        raw.release();

        out.add(request);
        queue.offer(msg);
    }

    private HttpRequest handleFlushRequest(ChannelHandlerContext ctx, FlushRequest msg) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, msg.path());
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
            handleBucketStreamingResponse(ctx, msg, out);
        } else if (currentRequest instanceof FlushRequest) {
            handleFlushResponse(ctx, msg, out);
        }
    }


    private void handleFlushResponse(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) {
        if (msg instanceof HttpResponse) {
            int code = ((HttpResponse) msg).getStatus().code();
            ResponseStatus status = code == 200 ? ResponseStatus.SUCCESS : ResponseStatus.FAILURE;
            boolean done = code != 201;
            out.add(new FlushResponse(done, status));
            currentRequest = null;
        }
    }

    private void handleBucketStreamingResponse(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) {
        if (msg instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) msg;
            if (response.getStatus().code() == 200) {
                configStream = PublishSubject.create();
                out.add(new BucketStreamingResponse(configStream, ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName(), ResponseStatus.SUCCESS, null));
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
        if (msg instanceof HttpResponse) {
            HttpResponse res = (HttpResponse) msg;
            currentStatus = res.getStatus().code();
        }
        if (msg instanceof HttpContent) {
            currentConfig.append(((HttpContent) msg).content().toString(CharsetUtil.UTF_8));

            if (msg instanceof LastHttpContent) {
                switch(currentStatus) {
                    case 200:
                        out.add(new BucketConfigResponse(currentConfig.toString(), ResponseStatus.SUCCESS));
                        break;
                    case 401:
                        out.add(new BucketConfigResponse("Unauthorized", ResponseStatus.FAILURE));
                        break;
                    case 404:
                        out.add(new BucketConfigResponse(currentConfig.toString(), ResponseStatus.NOT_EXISTS));
                        break;
                }

                currentConfig.setLength(0);
                currentRequest = null;
            }
        }
    }

}
