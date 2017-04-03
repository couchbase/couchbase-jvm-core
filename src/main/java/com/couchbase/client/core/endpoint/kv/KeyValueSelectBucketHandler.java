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

import java.net.SocketAddress;

import com.couchbase.client.core.endpoint.ServerFeatures;
import com.couchbase.client.core.endpoint.ServerFeaturesEvent;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * A Select bucket handler required for Spock cluster
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.4
 */
public class KeyValueSelectBucketHandler extends SimpleChannelInboundHandler<FullBinaryMemcacheResponse>
        implements ChannelOutboundHandler {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(KeyValueSelectBucketHandler.class);

    /**
     * The bucket to select.
     */
    private final String bucket;

    /**
     * The handler context.
     */
    private ChannelHandlerContext ctx;

    /**
     * The connect promise issued by the connect process.
     */
    private ChannelPromise originalPromise;

    /**
     * Boolean to be set based on HELLO response.
     */
    private boolean selectBucketEnabled;

    /**
     * Select bucket opcode
     */
    private static final byte SELECT_BUCKET_OPCODE = (byte)0x89;

    /**
     * Success response status
     */
    private static final byte SUCCESS = (byte)0x00;

    /**
     * Access error response status
     */
    private static final byte ACCESS_ERROR = (byte)0x24;

    /**
     * Creates a new {@link KeyValueSelectBucketHandler}.
     *
     * @param bucket the name of the user/bucket.
     */
    public KeyValueSelectBucketHandler(String bucket) {
        this.bucket = bucket;
    }

    /**
     * Once the channel is marked as active, select bucket command is sent if the HELLO request has SELECT_BUCKET feature
     * enabled.
     *
     * @param ctx the handler context.
     * @throws Exception if something goes wrong during communicating to the server.
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        if (selectBucketEnabled) {
            byte[] key = bucket.getBytes();
            short keyLength = (short) bucket.length();
            BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(key);
            request.setOpcode(SELECT_BUCKET_OPCODE);
            request.setKeyLength(keyLength);
            request.setTotalBodyLength(keyLength);
            this.ctx.writeAndFlush(request);
        } else {
            //remove the handler if the feature is not enabled
            originalPromise.setSuccess();
            this.ctx.pipeline().remove(this);
            this.ctx.fireChannelActive();
        }
    }

    /**
     * Handles incoming Select bucket responses.
     *
     * @param ctx the handler context.
     * @param msg the incoming message to investigate.
     * @throws Exception if something goes wrong during communicating to the server.
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg) throws Exception {
        switch (msg.getStatus()) {
            case SUCCESS:
                originalPromise.setSuccess();
                ctx.pipeline().remove(this);
                ctx.fireChannelActive();
                break;
            case ACCESS_ERROR:
                originalPromise.setFailure(new AuthenticationException("Authentication failure on Select Bucket command"));
                break;
            default:
                originalPromise.setFailure(new AuthenticationException("Unhandled select bucket status: "
                        + msg.getStatus()));
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof ServerFeaturesEvent) {
            selectBucketEnabled = ((ServerFeaturesEvent) evt).supportedFeatures().contains(ServerFeatures.SELECT_BUCKET);
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        ctx.bind(localAddress, promise);
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
}