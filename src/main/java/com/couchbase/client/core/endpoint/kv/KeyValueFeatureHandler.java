/**
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
package com.couchbase.client.core.endpoint.kv;

import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.ServerFeatures;
import com.couchbase.client.core.endpoint.ServerFeaturesEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * This handler negotiates the enabled features through the HELLO command.
 *
 * Like the SASL auth handler, this handler intercepts the original connect process to properly negotiate the
 * supported features with the server. Once the features are negotiated they are sent through custom events up the
 * pipeline and the handler removes itself.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class KeyValueFeatureHandler extends SimpleChannelInboundHandler<FullBinaryMemcacheResponse>
    implements ChannelOutboundHandler {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(KeyValueFeatureHandler.class);
    private static final byte HELLO_CMD = 0x1f;

    private final List<ServerFeatures> features;
    private final String userAgent;

    /**
     * The connect promise issued by the connect process.
     */
    private ChannelPromise originalPromise;

    public KeyValueFeatureHandler(CoreEnvironment environment) {
        userAgent = environment.userAgent();
        boolean tcpNodelay = environment.tcpNodelayEnabled();

        features = new ArrayList<ServerFeatures>();
        if (environment.mutationTokensEnabled()) {
            features.add(ServerFeatures.MUTATION_SEQNO);
        }
        features.add(tcpNodelay ? ServerFeatures.TCPNODELAY : ServerFeatures.TCPDELAY);

    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg) throws Exception {
        List<ServerFeatures> supported = new ArrayList<ServerFeatures>();

        ResponseStatus responseStatus = ResponseStatusConverter.fromBinary(msg.getStatus());
        if (responseStatus.isSuccess()) {
            while (msg.content().isReadable()) {
                supported.add(ServerFeatures.fromValue(msg.content().readShort()));
            }
        } else {
            LOGGER.debug("HELLO Negotiation did not succeed ({}).", responseStatus);
        }

        LOGGER.debug("Negotiated supported features: {}", supported);
        ctx.fireUserEventTriggered(new ServerFeaturesEvent(supported));
        originalPromise.setSuccess();
        ctx.pipeline().remove(this);
        ctx.fireChannelActive();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
       ctx.writeAndFlush(helloRequest());
    }

    /**
     * Creates the HELLO request to ask for certain supported features.
     *
     * @return the request to send over the wire
     */
    private FullBinaryMemcacheRequest helloRequest() {
        String key = userAgent;
        short keyLength = (short) key.getBytes(CharsetUtil.UTF_8).length;

        ByteBuf wanted = Unpooled.buffer(features.size() * 2);
        for (ServerFeatures feature : features) {
            wanted.writeShort(feature.value());
        }

        LOGGER.debug("Requesting supported features: {}", features);
        FullBinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(key, Unpooled.EMPTY_BUFFER, wanted);
        request.setOpcode(HELLO_CMD);
        request.setKeyLength(keyLength);
        request.setTotalBodyLength(keyLength + wanted.readableBytes());
        return request;
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
        ChannelPromise promise) throws Exception {
        originalPromise = promise;
        ChannelPromise downPromise = ctx.newPromise();
        downPromise.addListener(new GenericFutureListener<Future<Void>>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                if (!future.isSuccess() && !originalPromise.isDone()) {
                    originalPromise.setFailure(future.cause());
                }
            }
        });
        ctx.connect(remoteAddress, localAddress, downPromise);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        ctx.disconnect(promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        ctx.close(promise);
    }

    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        ctx.deregister(promise);
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        ctx.read();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ctx.write(msg, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        ctx.bind(localAddress, promise);
    }
}
