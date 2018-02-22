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
package com.couchbase.client.core.endpoint.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.ServerFeatures;
import com.couchbase.client.core.endpoint.ServerFeaturesEvent;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.HashMap;
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

    private static final ObjectMapper JACKSON = new ObjectMapper();
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(KeyValueFeatureHandler.class);
    private static final byte HELLO_CMD = 0x1f;

    private final List<ServerFeatures> features;
    private final CoreContext ctx;

    /**
     * The connect promise issued by the connect process.
     */
    private ChannelPromise originalPromise;

    public KeyValueFeatureHandler(CoreContext ctx) {
        boolean xerrorEnabled = Boolean.parseBoolean(
            System.getProperty("com.couchbase.xerrorEnabled", "true")
        );
        boolean snappyEnabled = Boolean.parseBoolean(
            System.getProperty("com.couchbase.snappyEnabled", "true")
        );
        this.ctx = ctx;
        boolean tcpNodelay = ctx.environment().tcpNodelayEnabled();

        features = new ArrayList<ServerFeatures>();
        if (ctx.environment().mutationTokensEnabled()) {
            features.add(ServerFeatures.MUTATION_SEQNO);
        }
        features.add(tcpNodelay ? ServerFeatures.TCPNODELAY : ServerFeatures.TCPDELAY);
        features.add(ServerFeatures.XATTR);
        features.add(ServerFeatures.SELECT_BUCKET);

        if (ctx.environment().tracingEnabled()) {
            features.add(ServerFeatures.TRACING);
        }

        if (snappyEnabled) {
            features.add(ServerFeatures.SNAPPY);
        }
        if (xerrorEnabled) {
            features.add(ServerFeatures.XERROR);
        }
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
       ctx.writeAndFlush(helloRequest(ctx.channel().hashCode()));
    }

    /**
     * Creates the HELLO request to ask for certain supported features.
     *
     * @param connId the connection id
     * @return the request to send over the wire
     */
    private FullBinaryMemcacheRequest helloRequest(int connId) throws Exception {
        byte[] key = generateAgentJson(
            ctx.environment().userAgent(),
            ctx.coreId(),
            connId
        );
        short keyLength = (short) key.length;

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

    /**
     * Helper method to generate the user agent JSON.
     */
    static byte[] generateAgentJson(String agent, long coreId, long channelId) throws Exception {
        String id = paddedHex(coreId) + "/" + paddedHex(channelId);
        if (agent.length() > 200) {
            agent = agent.substring(0, 200);
        }

        HashMap<String, String> result = new HashMap<String, String>();
        result.put("a", agent);
        result.put("i", id);
        return JACKSON.writeValueAsBytes(result);
    }

    /**
     * Helper method to create a padded hex long value.
     *
     * @param number number to pad.
     * @return the padded hex number.
     */
    public static String paddedHex(long number) {
        return String.format("%016X", number);
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
