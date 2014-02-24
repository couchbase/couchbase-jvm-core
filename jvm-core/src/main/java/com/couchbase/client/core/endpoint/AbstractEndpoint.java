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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.event.Event;

import java.net.InetSocketAddress;

/**
 * The {@link AbstractEndpoint} implements common functionality needed across all {@link Endpoint}s.
 *
 * This common functionality revolves mostly around connecting, disconnecting, state management, error handling and
 * logging. The actual endpoints should only implement the specific pipeline, keeping code duplication at a minimum.
 */
public abstract class AbstractEndpoint extends AbstractStateMachine<LifecycleState> implements Endpoint {

    /**
     * The logger to use for all endpoints.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Endpoint.class);

    /**
     * The couchbase environment.
     */
    private final Environment env;

    /**
     * The IO (netty) bootstrapper.
     */
    private final Bootstrap bootstrap;

    /**
     * The underlying IO (netty) channel.
     */
    private volatile Channel channel;

    /**
     * Add custom endpoint handlers to the {@link ChannelPipeline}.
     *
     * @param pipeline the pipeline where to add handlers.
     */
    protected abstract void customEndpointHandlers(final ChannelPipeline pipeline);

    /**
     * Create a new {@link Endpoint}.
     *
     * This also automatically sets up the IO bootstrap, but does not do the connection yet.
     *
     * @param address the address to connect.
     * @param env the environment to use.
     */
    protected AbstractEndpoint(final InetSocketAddress address, final Environment env) {
        super(LifecycleState.DISCONNECTED, env);

        this.env = env;

        bootstrap = new Bootstrap()
            .group(env.ioPool())
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();

                    if (LOGGER.isTraceEnabled()) {
                        pipeline.addLast(new DebugLoggingHandler(LogLevel.TRACE));
                    }
                    customEndpointHandlers(pipeline);
                    pipeline.addLast(new GenericEndpointHandler());
                }
            })
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.TCP_NODELAY, false)
            .remoteAddress(address);
    }

    @Override
    public Promise<LifecycleState> connect() {
        final Deferred<LifecycleState, Promise<LifecycleState>> deferred = Promises.defer(env.reactorEnv());
        bootstrap.connect().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    channel = future.channel();
                    transitionState(LifecycleState.CONNECTED);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Successfully connected to Endpoint " + channel.remoteAddress());
                    }
                    deferred.accept(state());
                }
                // TODO HANDLE ME FAILURE
            }
        });
        return deferred.compose();
    }

    @Override
    public Promise<LifecycleState> disconnect() {
        // TODO: handle disconnect
        return null;
    }

    @Override
    public <R extends CouchbaseResponse> Promise<R> send(final CouchbaseRequest request) {
        final Deferred<R,Promise<R>> deferred = Promises.defer(env.reactorEnv());

        if (state() == LifecycleState.CONNECTED) {
            Event<CouchbaseRequest> event = Event.wrap(request);
            event.setReplyTo(deferred);
            channel.write(event).addListener(new GenericFutureListener<Future<Void>>() {
                @Override
                public void operationComplete(final Future<Void> future) throws Exception {
                    if (!future.isSuccess()) {
                        // TODO: use better exceptions (also precreated ones for perf)
                        deferred.accept(new Exception("Writing the event into the endpoint failed", future.cause()));
                    }
                }
            });
        } else {
            // TODO: use better exceptions (also precreated ones for perf)
            deferred.accept(new Exception("Not connected :("));
        }

        return deferred.compose();
    }

}
