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
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.event.Event;

import java.net.InetSocketAddress;

public abstract class AbstractEndpoint extends AbstractStateMachine<LifecycleState> implements Endpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(Endpoint.class);
    private final Environment env;
    private final Bootstrap bootstrap;
    private volatile Channel channel;

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
                    //if (LOGGER.isTraceEnabled()) {
                        pipeline.addLast(new CustomLoggingHandler(LogLevel.DEBUG));
                    //}

                    customEndpointHandlers(pipeline);
                    pipeline.addLast(new GenericEndpointHandler());
                }
            })
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.TCP_NODELAY, false)
            .remoteAddress(address);
    }

    /**
     * Add custom endpoint handlers to the {@link ChannelPipeline}.
     *
     * @param pipeline the pipeline where to add handlers.
     */
    protected abstract void customEndpointHandlers(final ChannelPipeline pipeline);

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
    public <R extends CouchbaseResponse> Promise<R> send(CouchbaseRequest request) {
        Event<CouchbaseRequest> event = Event.wrap(request);
        Deferred<R,Promise<R>> deferred = Promises.defer(env.reactorEnv());
        event.setReplyTo(deferred);
        channel.write(event);
        // TODO: if writing fails, handle.
        return deferred.compose();
    }

}
