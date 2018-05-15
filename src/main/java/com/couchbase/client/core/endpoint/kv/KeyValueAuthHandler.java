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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.security.sasl.Sasl;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Arrays;

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
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(KeyValueAuthHandler.class);

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
     * If PLAIN SASL auth mode should be forced.
     */
    private final boolean forceSaslPlain;

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
    public KeyValueAuthHandler(String username, String password, boolean forceSaslPlain) {
        this.username = username;
        this.password = password == null ? "" : password;
        this.forceSaslPlain = forceSaslPlain;
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
     * @throws IOException if something goes wrong during negotiation.
     * @throws UnsupportedCallbackException if something goes wrong during negotiation.
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
     * @throws Exception if something goes wrong during negotiation.
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg) throws Exception {
        if (msg.getOpcode() == SASL_LIST_MECHS_OPCODE) {
            handleListMechsResponse(ctx, msg);
        }
        // It is necessary to call handleAuthResponse() for evaluate Auth Response and
        // Step Response returned by the server. If this step is not done, the client
        // doesn't authenticate the server.
        else if (msg.getOpcode() == SASL_AUTH_OPCODE || msg.getOpcode() == SASL_STEP_OPCODE) {
            handleAuthResponse(ctx, msg);
        }
    }

    /**
     * Handles an incoming SASL list mechanisms response and dispatches the next SASL AUTH step.
     *
     * @param ctx the handler context.
     * @param msg the incoming message to investigate.
     * @throws Exception if something goes wrong during negotiation.
     */
    private void handleListMechsResponse(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg) throws Exception {
        String remote = ctx.channel().remoteAddress().toString();
        String[] supportedMechanisms = msg.content().toString(CharsetUtil.UTF_8).split(" ");
        if (supportedMechanisms.length == 0) {
            throw new AuthenticationException("Received empty SASL mechanisms list from server: " + remote);
        }
        if (forceSaslPlain) {
            LOGGER.trace("Got SASL Mechs {} but forcing PLAIN due to config setting.", Arrays.asList(supportedMechanisms));
            supportedMechanisms = new String[] { "PLAIN" };
        }

        saslClient = Sasl.createSaslClient(supportedMechanisms, null, "couchbase", remote, null, this);
        selectedMechanism = saslClient.getMechanismName();
        int mechanismLength = selectedMechanism.length();
        byte[] bytePayload = saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[]{}) : null;
        ByteBuf payload = bytePayload != null ? ctx.alloc().buffer().writeBytes(bytePayload) : Unpooled.EMPTY_BUFFER;

        FullBinaryMemcacheRequest initialRequest = new DefaultFullBinaryMemcacheRequest(
            selectedMechanism.getBytes(CharsetUtil.UTF_8),
            Unpooled.EMPTY_BUFFER,
            payload
        );
        initialRequest
            .setOpcode(SASL_AUTH_OPCODE)
            .setKeyLength((short) mechanismLength)
            .setTotalBodyLength(mechanismLength + payload.readableBytes());

        ChannelFuture future = ctx.writeAndFlush(initialRequest);
        future.addListener(new GenericFutureListener<Future<Void>>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                if (!future.isSuccess()) {
                    LOGGER.warn("Error during SASL Auth negotiation phase.", future);
                }
            }
        });
    }

    /**
     * Handles an incoming SASL AUTH response and - if needed - dispatches the SASL STEPs.
     *
     * @param ctx the handler context.
     * @param msg the incoming message to investigate.
     * @throws Exception if something goes wrong during negotiation.
     */
    private void handleAuthResponse(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg) throws Exception {
        byte[] response = new byte[msg.content().readableBytes()];
        msg.content().readBytes(response);

        // Under PLAIN it might be already complete, so bail out early if so.
        // Also, make sure that auth failure response codes are covered early
        // and bailed out before evaluating the challenge.
        if (saslClient.isComplete() || msg.getStatus() == SASL_AUTH_FAILURE) {
            checkIsAuthed(msg);
            return;
        }

        byte[] evaluatedBytes = saslClient.evaluateChallenge(response);
        
        // This condition is needed for evaluate SASL Step Response. 
        // After saslClient.evaluateChallenge(), if SaslException is not thrown and 
        // if ShaSaslClient.serverFinalMessage is not null, authentication is completed
        if (saslClient.isComplete()) {
            checkIsAuthed(msg);
            return;
        }

        if (evaluatedBytes != null) {
            ByteBuf content;

            // This is needed against older server versions where the protocol does not
            // align on cram and plain, the else block is used for all the newer cram-sha*
            // mechanisms.
            //
            // Note that most likely this is only executed in the CRAM-MD5 case only, but
            // just to play it safe keep it for both mechanisms.
            if (selectedMechanism.equals("CRAM-MD5") || selectedMechanism.equals("PLAIN")) {
                String[] evaluated = new String(evaluatedBytes).split(" ");
                content = Unpooled.copiedBuffer(username + "\0" + evaluated[1], CharsetUtil.UTF_8);
            } else {
                content = Unpooled.wrappedBuffer(evaluatedBytes);
            }

            FullBinaryMemcacheRequest stepRequest = new DefaultFullBinaryMemcacheRequest(
                selectedMechanism.getBytes(CharsetUtil.UTF_8),
                Unpooled.EMPTY_BUFFER,
                content
            );
            stepRequest
                .setOpcode(SASL_STEP_OPCODE)
                .setKeyLength((short) selectedMechanism.length())
                .setTotalBodyLength(content.readableBytes() + selectedMechanism.length());

            ChannelFuture future = ctx.writeAndFlush(stepRequest);
            future.addListener(new GenericFutureListener<Future<Void>>() {
                @Override
                public void operationComplete(Future<Void> future) throws Exception {
                    if (!future.isSuccess()) {
                        LOGGER.warn("Error during SASL Auth negotiation phase.", future);
                    }
                }
            });
        } else {
            throw new AuthenticationException("SASL Challenge evaluation returned null.");
        }
    }

    /**
     * Once authentication is completed, check the response and react appropriately
     * to the upper layers.
     *
     * @param msg the incoming message to investigate.
     */
    private void checkIsAuthed(final FullBinaryMemcacheResponse msg) {
        switch (msg.getStatus()) {
            case SASL_AUTH_SUCCESS:
                originalPromise.setSuccess();
                ctx.pipeline().remove(this);
                ctx.fireChannelActive();
                break;
            case SASL_AUTH_FAILURE:
                originalPromise.setFailure(new AuthenticationException("Authentication Failure"));
                break;
            default:
                originalPromise.setFailure(new AuthenticationException("Unhandled SASL auth status: "
                    + msg.getStatus()));
        }
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
