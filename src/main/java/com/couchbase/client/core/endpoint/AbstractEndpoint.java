package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.NotConnectedException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.AsyncSubject;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractEndpoint extends AbstractStateMachine<LifecycleState> implements Endpoint {

    /**
     * The logger to use for all endpoints.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Endpoint.class);

    /**
     * A shared logging handler for all endpoints.
     */
    private static final ChannelHandler LOGGING_HANDLER_INSTANCE = new DebugLoggingHandler(LogLevel.TRACE);

    /**
     * Precreated not connected exception for performance reasons.
     */
    private static final NotConnectedException NOT_CONNECTED_EXCEPTION = new NotConnectedException();

    private final BootstrapAdapter bootstrap;

    /**
     * The reconnect delay that increases with backoff.
     */
    private final AtomicLong reconnectDelay = new AtomicLong(0);

    /**
     * The underlying IO (netty) channel.
     */
    private volatile Channel channel;

    /**
     * Preset the stack trace for the static exceptions.
     */
    static {
        NOT_CONNECTED_EXCEPTION.setStackTrace(new StackTraceElement[0]);
    }

    protected AbstractEndpoint(BootstrapAdapter adapter) {
        super(LifecycleState.DISCONNECTED);
        bootstrap = adapter;
    }

    protected AbstractEndpoint(String hostname, Environment environment) {
        super(LifecycleState.DISCONNECTED);

        final ChannelHandler endpointHandler = new GenericEndpointHandler(this);
        bootstrap = new BootstrapAdapter(new Bootstrap()
            .remoteAddress(hostname, port())
            .group(environment.ioPool())
            .channel(NioSocketChannel.class)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.TCP_NODELAY, false)
            .handler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel channel) throws Exception {
                    ChannelPipeline pipeline = channel.pipeline();
                    if (LOGGER.isTraceEnabled()) {
                        pipeline.addLast(LOGGING_HANDLER_INSTANCE);
                    }
                    customEndpointHandlers(pipeline);
                    pipeline.addLast(endpointHandler);
                }
            }));
    }

    /**
     * Returns the port of the endpoint.
     *
     * @return the port of the endpoint.
     */
    protected abstract int port();

    /**
     * Add custom endpoint handlers to the {@link ChannelPipeline}.
     *
     * @param pipeline the pipeline where to add handlers.
     */
    protected abstract void customEndpointHandlers(ChannelPipeline pipeline);

    @Override
    public Observable<LifecycleState> connect() {
        if (state() != LifecycleState.DISCONNECTED) {
            return Observable.from(state());
        }

        final AsyncSubject<LifecycleState> observable = AsyncSubject.create();
        transitionState(LifecycleState.CONNECTING);
        bootstrap.connect().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                if (state() == LifecycleState.DISCONNECTING || state() == LifecycleState.DISCONNECTED) {
                    LOGGER.debug("Endpoint connect completed, but got instructed to disconnect in the meantime.");
                    transitionState(LifecycleState.DISCONNECTED);
                    channel = null;
                } else {
                    if (future.isSuccess()) {
                        channel = future.channel();
                        transitionState(LifecycleState.CONNECTED);
                        LOGGER.debug("Successfully connected to Endpoint " + channel.remoteAddress());
                    } else {
                        long delay = reconnectDelay();
                        LOGGER.warn("Could not connect to endpoint, retrying with delay " + delay + "ms: "
                            + future.channel().remoteAddress(), future.cause());
                        transitionState(LifecycleState.CONNECTING);
                        future.channel().eventLoop().schedule(new Runnable() {
                            @Override
                            public void run() {
                                connect();
                            }
                        }, delay, TimeUnit.MILLISECONDS);
                    }
                }
                observable.onNext(state());
                observable.onCompleted();
            }
        });
        return observable;
    }

    @Override
    public Observable<LifecycleState> disconnect() {
        if (state() == LifecycleState.DISCONNECTED || state() == LifecycleState.DISCONNECTING) {
            return Observable.from(state());
        }

        if (state() == LifecycleState.CONNECTING) {
            transitionState(LifecycleState.DISCONNECTED);
            return Observable.from(state());
        }

        transitionState(LifecycleState.DISCONNECTING);
        final AsyncSubject<LifecycleState> observable = AsyncSubject.create();
        channel.disconnect().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    LOGGER.debug("Successfully disconnected from Endpoint " + channel.remoteAddress());
                } else {
                    LOGGER.warn("Received an error during disconnect.", future.cause());
                }
                transitionState(LifecycleState.DISCONNECTED);
                observable.onNext(state());
                observable.onCompleted();
                channel = null;
            }
        });
        return observable;
    }

    @Override
    public void send(final CouchbaseRequest request) {
        if (state() == LifecycleState.CONNECTED) {
            if (request instanceof SignalFlush) {
                channel.flush();
            } else {
                channel.write(request).addListener(new GenericFutureListener<Future<Void>>() {
                    @Override
                    public void operationComplete(Future<Void> future) throws Exception {
                        if (!future.isSuccess()) {
                            request.observable().onError(future.cause());
                        }
                    }
                });
            }
        } else {
            request.observable().onError(NOT_CONNECTED_EXCEPTION);
        }
    }

    /**
     * Helper method that is called inside from the event loop to notify the upper endpoint of a disconnect.
     *
     * Subsequent reconnect attempts are triggered from here.
     */
    void notifyChannelInactive() {
        transitionState(LifecycleState.DISCONNECTED);
        connect();
    }

    /**
     * Returns the reconnect retry delay in Miliseconds.
     *
     * Currently, it uses linear backoff.
     *
     * @return the retry delay.
     */
    private long reconnectDelay() {
        return reconnectDelay.getAndIncrement();
    }
}
