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
package com.couchbase.client.core.endpoint.kv;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.ServerFeatures;
import com.couchbase.client.core.endpoint.ServerFeaturesEvent;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
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

/**
 * This handler is added dynamically by the {@link KeyValueFeatureHandler} to load and store the extended
 * error map from the server. It will only be added if the server has this feature enabled.
 *
 * @author Michael Nitschinger
 * @since 1.4.4
 */
public class KeyValueErrorMapHandler extends SimpleChannelInboundHandler<FullBinaryMemcacheResponse>
        implements ChannelOutboundHandler {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(KeyValueErrorMapHandler.class);
    private static final byte GET_ERROR_MAP_CMD = (byte) 0xfe;
    private static final short MAP_VERSION = 1; // first version of the error map

    private boolean errorMapEnabled;

    /**
     * The connect promise issued by the connect process.
     */
    private ChannelPromise originalPromise;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg) throws Exception {
        if (KeyValueStatus.SUCCESS.code() == msg.getStatus()) {
            String content = msg.content().toString(CharsetUtil.UTF_8);
            ErrorMap errorMap = ErrorMap.fromJson(content);
            LOGGER.debug("Trying to update Error Map With Version {}, Revision {}.",
                errorMap.version(), errorMap.revision());
            ResponseStatusConverter.updateBinaryErrorMap(errorMap);
            originalPromise.setSuccess();
            ctx.pipeline().remove(this);
            ctx.fireChannelActive();
        } else {
            LOGGER.warn("Could not load extended error map, because the server responded with an error. Error " +
                "code {}", Integer.toHexString(msg.getStatus()));
            originalPromise.setFailure(new CouchbaseException("Could not load extended error map, " +
                "because the server responded with an error. "));
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (errorMapEnabled) {
            ctx.writeAndFlush(errorMapRequest());
        } else {
            originalPromise.setSuccess();
            ctx.pipeline().remove(this);
            ctx.fireChannelActive();
        }
    }

    /**
     * Creates the request to load the error map.
     */
    private FullBinaryMemcacheRequest errorMapRequest() {
        LOGGER.debug("Requesting error map in version {}", MAP_VERSION);
        ByteBuf content = Unpooled.buffer(2).writeShort(MAP_VERSION);
        FullBinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(
            new byte[] {}, Unpooled.EMPTY_BUFFER, content
        );
        request.setOpcode(GET_ERROR_MAP_CMD);
        request.setTotalBodyLength(content.readableBytes());
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
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof ServerFeaturesEvent) {
            errorMapEnabled = ((ServerFeaturesEvent) evt).supportedFeatures()
                .contains(ServerFeatures.XERROR);
        }
        super.userEventTriggered(ctx, evt);
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
