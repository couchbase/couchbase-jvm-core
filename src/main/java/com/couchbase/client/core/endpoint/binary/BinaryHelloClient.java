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
package com.couchbase.client.core.endpoint.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

/**
 * This handler is responsible for negotiating the HELLO command after connect and authentication.
 *
 * Depending on the result, it sets certain flags that can be used throughout the pipeline to influence
 * the process of creating and compressing data (including using the datatype field).
 *
 * Once its done, it removes itself from the pipeline.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class BinaryHelloClient extends SimpleChannelInboundHandler<FullBinaryMemcacheResponse> implements ChannelOutboundHandler {

    private static final byte HELLO_CMD = 0x1F;
    private static final byte DATATYPE_SUPPORT = 0x01;

    /**
     * The logger in use.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(BinaryHelloClient.class);

    /**
     * The connect promise issued by the connect process.
     */
    private ChannelPromise originalPromise;

    /**
     * The channel handler context.
     */
    private ChannelHandlerContext ctx;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg) throws Exception {
        if (msg.getStatus() == BinaryMemcacheResponseStatus.SUCCESS) {
            while(msg.content().isReadable()) {
                short supported = msg.content().readShort();
                if (supported == DATATYPE_SUPPORT) {
                    SupportedDatatypes dtypes = new SupportedDatatypes(true, true);
                    LOGGER.debug(ctx.channel().remoteAddress() + " Hello detected: " + dtypes);
                    ctx.fireUserEventTriggered(dtypes);
                }
            }
        } else {
            LOGGER.debug(ctx.channel().remoteAddress() + " Hello not successful (Response Status: "
                + msg.getStatus() + "), falling back to " + "no datatype.");
        }

        originalPromise.setSuccess();
        ctx.pipeline().remove(this);
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        ctx.bind(localAddress, promise);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    private void initHello() {
        ByteBuf content = ctx.alloc().buffer(2);
        content.writeShort(DATATYPE_SUPPORT);

        String key = "Couchbase Java SDK";
        BinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(key, Unpooled.EMPTY_BUFFER, content);
        request.setOpcode(HELLO_CMD);
        request.setTotalBodyLength(content.readableBytes() + key.length());
        request.setKeyLength((short) key.length());
        ctx.writeAndFlush(request);
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        originalPromise = promise;
        ChannelPromise downPromise = ctx.newPromise();
        downPromise.addListener(new GenericFutureListener<Future<Void>>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                if (future.isSuccess()) {
                    initHello();
                }
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
