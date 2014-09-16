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
package com.couchbase.client.core.endpoint.kv;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import java.io.IOException;
import java.net.SocketAddress;

/**
 * A SASL Client which communicates through the memcache binary protocol.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class KeyValueAuthHandler
    extends SimpleChannelInboundHandler<FullBinaryMemcacheResponse>
    implements CallbackHandler, ChannelOutboundHandler {

    /**
     * The memcache opcode for the SASL mechs list.
     */
    public static final byte SASL_LIST_MECHS_OPCODE = 0x20;

    /**
     * The memcache opcode for the SASL initial auth.
     */
    private static final byte SASL_AUTH_OPCODE = 0x21;

    /**
     * The memcache opcode for the SASL consecutive steps.
     */
    private static final byte SASL_STEP_OPCODE = 0x22;

    /**
     * The server response indicating SASL auth success.
     */
    private static final byte SASL_AUTH_SUCCESS = BinaryMemcacheResponseStatus.SUCCESS;

    /**
     * The server response indicating SASL auth failure.
     */
    private static final byte SASL_AUTH_FAILURE = BinaryMemcacheResponseStatus.AUTH_ERROR;

    /**
     * The username to auth against.
     */
    private final String username;

    /**
     * The password of the user.
     */
    private final String password;

    /**
     * The handler context.
     */
    private ChannelHandlerContext ctx;

    /**
     * The JVM {@link SaslClient} that handles the actual authentication process.
     */
    private SaslClient saslClient;

    /**
     * Contains the selected SASL auth mechanism once decided.
     */
    private String selectedMechanism;

    /**
     * The connect promise issued by the connect process.
     */
    private ChannelPromise originalPromise;

    /**
     * Creates a new {@link KeyValueAuthHandler}.
     *
     * @param username the name of the user/bucket.
     * @param password the password associated with the user/bucket.
     */
    public KeyValueAuthHandler(String username, String password) {
        this.username = username;
        this.password = password == null ? "" : password;
    }

    /**
     * Once the channel is marked as active, the SASL negotiation is started.
     *
     * @param ctx the handler context.
     * @throws Exception if something goes wrong during negotiation.
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        ctx.writeAndFlush(new DefaultBinaryMemcacheRequest().setOpcode(SASL_LIST_MECHS_OPCODE));
    }

    /**
     * Callback handler needed for the {@link SaslClient} which supplies username and password.
     *
     * @param callbacks the possible callbacks.
     * @throws IOException
     * @throws UnsupportedCallbackException
     */
    @Override
    public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(username);
            } else if (callback instanceof PasswordCallback) {
                ((PasswordCallback) callback).setPassword(password.toCharArray());
            } else {
                throw new AuthenticationException("SASLClient requested unsupported callback: " + callback);
            }
        }
    }

    /**
     * Dispatches incoming SASL responses to the appropriate handler methods.
     *
     * @param ctx the handler context.
     * @param msg the incoming message to investigate.
     * @throws Exception
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg) throws Exception {
        if (msg.getOpcode() == SASL_LIST_MECHS_OPCODE) {
            handleListMechsResponse(ctx, msg);
        } else if (msg.getOpcode() == SASL_AUTH_OPCODE) {
            handleAuthResponse(ctx, msg);
        } else if (msg.getOpcode() == SASL_STEP_OPCODE) {
            checkIsAuthed(msg);
        }
    }

    /**
     * Handles an incoming SASL list mechanisms response and dispatches the next SASL AUTH step.
     *
     * @param ctx the handler context.
     * @param msg the incoming message to investigate.
     * @throws Exception
     */
    private void handleListMechsResponse(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg) throws Exception {
        String remote = ctx.channel().remoteAddress().toString();
        String[] supportedMechanisms = msg.content().toString(CharsetUtil.UTF_8).split(" ");
        if (supportedMechanisms.length == 0) {
            throw new AuthenticationException("Received empty SASL mechanisms list from server: " + remote);
        }

        saslClient = Sasl.createSaslClient(supportedMechanisms, null, "couchbase", remote, null, this);
        selectedMechanism = saslClient.getMechanismName();
        int mechanismLength = selectedMechanism.length();
        byte[] bytePayload = saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[]{}) : null;
        ByteBuf payload = bytePayload != null ? ctx.alloc().buffer().writeBytes(bytePayload) : Unpooled.EMPTY_BUFFER;

        FullBinaryMemcacheRequest initialRequest = new DefaultFullBinaryMemcacheRequest(
            selectedMechanism,
            Unpooled.EMPTY_BUFFER,
            payload
        );
        initialRequest
            .setOpcode(SASL_AUTH_OPCODE)
            .setKeyLength((short) mechanismLength)
            .setTotalBodyLength(mechanismLength + payload.readableBytes());

        ctx.writeAndFlush(initialRequest);
    }

    /**
     * Handles an incoming SASL AUTH response and - if needed - dispatches the SASL STEPs.
     *
     * @param ctx the handler context.
     * @param msg the incoming message to investigate.
     * @throws Exception
     */
    private void handleAuthResponse(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg) throws Exception {
        if (saslClient.isComplete()) {
            checkIsAuthed(msg);
            return;
        }

        byte[] response = new byte[msg.content().readableBytes()];
        msg.content().readBytes(response);
        byte[] evaluatedBytes = saslClient.evaluateChallenge(response);

        if (evaluatedBytes != null) {
            String[] evaluated = new String(evaluatedBytes).split(" ");
            ByteBuf content = Unpooled.copiedBuffer(username + "\0" + evaluated[1], CharsetUtil.UTF_8);

            FullBinaryMemcacheRequest stepRequest = new DefaultFullBinaryMemcacheRequest(
                selectedMechanism,
                Unpooled.EMPTY_BUFFER,
                content
            );
            stepRequest
                .setOpcode(SASL_STEP_OPCODE)
                .setKeyLength((short) selectedMechanism.length())
                .setTotalBodyLength(content.readableBytes() + selectedMechanism.length());

            ctx.writeAndFlush(stepRequest);
        } else {
            throw new AuthenticationException("SASL Challenge evaluation returned null.");
        }
    }

    /**
     * Once authentication is completed, check the response and react appropriately to the upper layers.
     *
     * @param msg the incoming message to investigate.
     */
    private void checkIsAuthed(final FullBinaryMemcacheResponse msg) {
        switch (msg.getStatus()) {
            case SASL_AUTH_SUCCESS:
                originalPromise.setSuccess();
                ctx.pipeline().remove(this);
                break;
            case SASL_AUTH_FAILURE:
                originalPromise.setFailure(new AuthenticationException("Authentication Failure"));
                break;
            default:
                originalPromise.setFailure(new AuthenticationException("Unhandled SASL auth status: " + msg.getStatus()));
        }
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        ctx.bind(localAddress, promise);
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
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
