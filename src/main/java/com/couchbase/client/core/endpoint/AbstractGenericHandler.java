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

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.ResponseHandler;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.CoreScheduler;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.metrics.NetworkLatencyMetricsIdentifier;
import com.couchbase.client.core.service.ServiceType;
import com.lmax.disruptor.EventSink;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;
import rx.Scheduler;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.subjects.Subject;

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Generic handler which acts as the common base type for all implementing handlers.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public abstract class AbstractGenericHandler<RESPONSE, ENCODED, REQUEST extends CouchbaseRequest>
    extends MessageToMessageCodec<RESPONSE, REQUEST> {

    /**
     * The default charset to use for all requests and responses.
     */
    protected static final Charset CHARSET = CharsetUtil.UTF_8;

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(AbstractGenericHandler.class);

    /**
     * Empty bytes to reuse.
     */
    protected static final byte[] EMPTY_BYTES = new byte[] {};

    /**
     * The response buffer to push response events into.
     */
    private final EventSink<ResponseEvent> responseBuffer;

    /**
     * The endpoint held as a reference.
     */
    private final AbstractEndpoint endpoint;

    /**
     * This queue keeps all currently outstanding requests.
     */
    private final Queue<REQUEST> sentRequestQueue;

    /**
     * This queue keeps all timings for each request when it was sent off to the event loop.
     */
    private final Queue<Long> sentRequestTimings;

    /**
     * If this handler is transient (will close after one request).
     */
    private final boolean isTransient;

    /**
     * If TRACE level logging has been enabled at startup.
     */
    private final boolean traceEnabled;

    /**
     * If the response need to be moved out of the event loop.
     */
    private final boolean moveResponseOut;

    /**
     * A cache to avoid consistent string conversions for the request simple names.
     */
    private final Map<Class<? extends CouchbaseRequest>, String> classNameCache;

    /**
     * The request which is expected to return next.
     */
    private REQUEST currentRequest;

    private DecodingState currentDecodingState;

    /**
     * Contains the current round-trip-time for the last completed operation. Used for metrics.
     */
    private long currentOpTime = -1;

    /**
     * Contains the stringified version of the remote node's hostname. Used for metrics.
     */
    private String remoteHostname;

    /**
     * The future which is used to eventually signal a connected channel.
     */
    private ChannelPromise connectFuture;

    /**
     * Returns the remote http host in usable format.
     */
    private String remoteHttpHost;

    /**
     * Creates a new {@link AbstractGenericHandler} with the default queue.
     *
     * @param endpoint the endpoint reference.
     * @param responseBuffer the response buffer.
     */
    protected AbstractGenericHandler(final AbstractEndpoint endpoint, final EventSink<ResponseEvent> responseBuffer, final boolean isTransient) {
        this(endpoint, responseBuffer, new ArrayDeque<REQUEST>(), isTransient);
    }

    /**
     * Creates a new {@link AbstractGenericHandler} with a custom queue.
     *
     * @param endpoint the endpoint reference.
     * @param responseBuffer the response buffer.
     * @param queue the queue.
     */
    protected AbstractGenericHandler(final AbstractEndpoint endpoint, final EventSink<ResponseEvent> responseBuffer,
        final Queue<REQUEST> queue, final boolean isTransient) {
        this.endpoint = endpoint;
        this.responseBuffer = responseBuffer;
        this.sentRequestQueue = queue;
        this.currentDecodingState = DecodingState.INITIAL;
        this.isTransient = isTransient;
        this.traceEnabled = LOGGER.isTraceEnabled();
        this.sentRequestTimings = new ArrayDeque<Long>();
        this.classNameCache = new IdentityHashMap<Class<? extends CouchbaseRequest>, String>();
        this.moveResponseOut = env() == null || !env().callbacksOnIoPool();
    }

    /**
     * Encode the outgoing request and return it in encoded format.
     *
     * This method needs to be implemented by the child handler and is responsible for the actual conversion.
     *
     * @param ctx the context passed in.
     * @param msg the outgoing message.
     * @return the encoded request.
     * @throws Exception as a generic error.
     */
    protected abstract ENCODED encodeRequest(ChannelHandlerContext ctx, REQUEST msg) throws Exception;

    /**
     * Decodes the incoming response and transforms it into a {@link CouchbaseResponse}.
     *
     * Note that the actual notification is handled by this generic handler, the implementing class only is concerned
     * about the conversion itself.
     *
     * @param ctx the context passed in.
     * @param msg the incoming message.
     * @return a response or null if nothing should be returned.
     * @throws Exception as a generic error. It will be bubbled up to the user (wrapped in a CouchbaseException) in the
     *   onError of the request's Observable.
     */
    protected abstract CouchbaseResponse decodeResponse(ChannelHandlerContext ctx, RESPONSE msg) throws Exception;

    /**
     * Returns the {@link ServiceType} associated with this handler.
     *
     * @return the service type.
     */
    protected abstract ServiceType serviceType();

    @Override
    protected void encode(ChannelHandlerContext ctx, REQUEST msg, List<Object> out) throws Exception {
        ENCODED request = encodeRequest(ctx, msg);
        sentRequestQueue.offer(msg);
        out.add(request);
        sentRequestTimings.offer(System.nanoTime());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, RESPONSE msg, List<Object> out) throws Exception {
        if (currentDecodingState == DecodingState.INITIAL) {
            initialDecodeTasks(ctx);
        }

        try {
            CouchbaseResponse response = decodeResponse(ctx, msg);
            if (response != null) {
                publishResponse(response, currentRequest.observable());
                if (currentDecodingState == DecodingState.FINISHED) {
                    writeMetrics(response);
                }
            }
        } catch (CouchbaseException e) {
            currentRequest.observable().onError(e);
        } catch (Exception e) {
            currentRequest.observable().onError(new CouchbaseException(e));
        }

        if (currentDecodingState == DecodingState.FINISHED) {
            resetStatesAfterDecode(ctx);
        }
    }

    /**
     * Helper method which creates the metrics for the current response and publishes them if enabled.
     *
     * @param response the response which is needed as context.
     */
    private void writeMetrics(final CouchbaseResponse response) {
        if (currentRequest != null && currentOpTime >= 0 && env() != null
            && env().networkLatencyMetricsCollector().isEnabled()) {

            try {
                Class<? extends CouchbaseRequest> requestClass = currentRequest.getClass();
                String simpleName = classNameCache.get(requestClass);
                if (simpleName == null) {
                    simpleName = requestClass.getSimpleName();
                    classNameCache.put(requestClass, simpleName);
                }

                NetworkLatencyMetricsIdentifier identifier = new NetworkLatencyMetricsIdentifier(
                        remoteHostname,
                        serviceType().toString(),
                        simpleName,
                        response.status().toString()
                );
                env().networkLatencyMetricsCollector().record(identifier, currentOpTime);
            } catch (Throwable e) {
                LOGGER.warn("Could not collect latency metric for request + "
                    + currentRequest + "(" + currentOpTime + ")", e);
            }
        }
    }

    /**
     * Helper method which performs the final tasks in the decoding process.
     *
     * @param ctx the channel handler context for logging purposes.
     */
    private void resetStatesAfterDecode(final ChannelHandlerContext ctx) {
        if (traceEnabled) {
            LOGGER.trace("{}Finished decoding of {}", logIdent(ctx, endpoint), currentRequest);
        }
        currentRequest = null;
        currentDecodingState = DecodingState.INITIAL;
    }

    /**
     * Helper method which performs the initial decoding process.
     *
     * @param ctx the channel handler context for logging purposes.
     */
    private void initialDecodeTasks(final ChannelHandlerContext ctx) {
        currentRequest = sentRequestQueue.poll();
        currentDecodingState = DecodingState.STARTED;

        if (currentRequest != null) {
            Long st = sentRequestTimings.poll();
            if (st != null) {
                currentOpTime = System.nanoTime() - st;
            } else {
                currentOpTime = -1;
            }
        }

        if (traceEnabled) {
            LOGGER.trace("{}Started decoding of {}", logIdent(ctx, endpoint), currentRequest);
        }
    }

    /**
     * Publishes a response with the attached observable.
     *
     * @param response the response to publish.
     * @param observable pushing into the event sink.
     */
    protected void publishResponse(final CouchbaseResponse response,
        final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        if (response.status() != ResponseStatus.RETRY && observable != null) {
            if (moveResponseOut) {
                Scheduler scheduler = env().scheduler();
                if (scheduler instanceof CoreScheduler) {
                    scheduleDirect((CoreScheduler) scheduler, response, observable);
                } else {
                    scheduleWorker(scheduler, response, observable);
                }
            } else {
                completeResponse(response, observable);
            }
        } else {
            responseBuffer.publishEvent(ResponseHandler.RESPONSE_TRANSLATOR, response, observable);
        }
    }

    /**
     * Fulfill and complete the response observable.
     *
     * When called directly, this method completes on the event loop, but it can also be used in a callback (see
     * {@link #scheduleDirect(CoreScheduler, CouchbaseResponse, Subject)} for example.
     */
    private static void completeResponse(final CouchbaseResponse response,
        final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        try {
            observable.onNext(response);
            observable.onCompleted();
        } catch (Exception ex) {
            LOGGER.warn("Caught exception while onNext on observable", ex);
            observable.onError(ex);
        }
    }

    /**
     * Optimized version of dispatching onto the core scheduler through direct scheduling.
     *
     * This method has less GC overhead compared to {@link #scheduleWorker(Scheduler, CouchbaseResponse, Subject)}
     * since no worker needs to be generated explicitly (but is not part of the public Scheduler interface).
     */
    private static void scheduleDirect(CoreScheduler scheduler, final CouchbaseResponse response,
        final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        scheduler.scheduleDirect(new Action0() {
            @Override
            public void call() {
                completeResponse(response, observable);
            }
        });
    }

    /**
     * Dispatches the response on a generic scheduler through creating a worker.
     */
    private static void scheduleWorker(Scheduler scheduler, final CouchbaseResponse response,
        final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        final Scheduler.Worker worker = scheduler.createWorker();
        worker.schedule(new Action0() {
            @Override
            public void call() {
                try {
                    observable.onNext(response);
                    observable.onCompleted();
                } catch (Exception ex) {
                    LOGGER.warn("Caught exception while onNext on observable", ex);
                    observable.onError(ex);
                } finally {
                    worker.unsubscribe();
                }
            }
        });
    }

    /**
     * Notify that decoding is finished. This needs to be called by the child handlers in order to
     * signal that operations are done.
     */
    protected void finishedDecoding() {
        this.currentDecodingState = DecodingState.FINISHED;
        if (isTransient) {
            endpoint.disconnect();
        }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        LOGGER.debug(logIdent(ctx, endpoint) + "Channel Inactive.");
        endpoint.notifyChannelInactive();
        ctx.fireChannelInactive();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.debug(logIdent(ctx, endpoint) + "Channel Active.");
        remoteHostname = ctx.channel().remoteAddress().toString();
        ctx.fireChannelActive();
    }

    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
        if (!ctx.channel().isWritable()) {
            ctx.flush();
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
        ChannelPromise future) throws Exception {
        connectFuture = future;
        ctx.connect(remoteAddress, localAddress, future);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(logIdent(ctx, endpoint) + "Connection reset by peer: " + cause.getMessage(), cause);
            } else {
                LOGGER.info(logIdent(ctx, endpoint) + "Connection reset by peer: " + cause.getMessage());
            }
            handleOutstandingOperations(ctx);
        } else if (cause instanceof DecoderException && cause.getCause() instanceof SSLHandshakeException) {
            if (!connectFuture.isDone()) {
                connectFuture.setFailure(cause.getCause());
            } else {
                // This should not be possible, since handshake is done before connecting. But just in case, we
                // can trap and log an error that might slip through for one reason or another.
                LOGGER.warn(logIdent(ctx, endpoint) + "Caught SSL exception after being connected: "
                    + cause.getMessage(), cause);
            }
        } else {
            LOGGER.warn(logIdent(ctx, endpoint) + "Caught unknown exception: " + cause.getMessage(), cause);
            ctx.fireExceptionCaught(cause);
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        handleOutstandingOperations(ctx);
    }

    /**
     * Cancells any outstanding operations which are currently on the wire.
     *
     * @param ctx the handler context.
     */
    private void handleOutstandingOperations(final ChannelHandlerContext ctx) {
        if (sentRequestQueue.isEmpty()) {
            LOGGER.trace(logIdent(ctx, endpoint) + "Not cancelling operations - sent queue is empty.");
            return;
        }

        LOGGER.debug(logIdent(ctx, endpoint) + "Cancelling " + sentRequestQueue.size() + " outstanding requests.");
        while (!sentRequestQueue.isEmpty()) {
            REQUEST req = sentRequestQueue.poll();
            try {
                sideEffectRequestToCancel(req);
                req.observable().onError(new RequestCancelledException("Request cancelled in-flight."));
            } catch (Exception ex) {
                LOGGER.info("Exception thrown while cancelling outstanding operation: " + req, ex);
            }
        }

        sentRequestTimings.clear();
    }


    /**
     * This method can be overridden as it is called every time an operation is cancelled.
     *
     * Overriding implementations may do some custom logic with them, for example freeing resources they know of
     * to avoid leaking.
     *
     * @param request the request to side effect on.
     */
    protected void sideEffectRequestToCancel(final REQUEST request) {
        // Nothing to do in the generic implementation.
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.ALL_IDLE) {
                CouchbaseRequest keepAlive = createKeepAliveRequest();
                if (keepAlive != null) {
                    keepAlive.observable().subscribe(new KeepAliveResponseAction(ctx));
                    onKeepAliveFired(ctx, keepAlive);

                    Channel channel = ctx.channel();
                    if (channel.isActive() && channel.isWritable()) {
                        ctx.pipeline().writeAndFlush(keepAlive);
                    }
                }
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    /**
     * Override to return a non-null request to be fired in the pipeline in case a keep alive is triggered.
     *
     * @return a CouchbaseRequest to be fired in case of keep alive (null by default).
     */
    protected CouchbaseRequest createKeepAliveRequest() {
        return null;
    }

    /**
     * Override to customize the behavior when a keep alive has been triggered and a keep alive request sent.
     *
     * The default behavior is to log the event at debug level.
     *
     * @param ctx the channel context.
     * @param keepAliveRequest the keep alive request that was sent when keep alive was triggered
     */
    protected void onKeepAliveFired(ChannelHandlerContext ctx, CouchbaseRequest keepAliveRequest) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(logIdent(ctx, endpoint) + "KeepAlive fired");
        }
    }

    /**
     * Override to customize the behavior when a keep alive has been responded to.
     *
     * The default behavior is to log the event and the response status at trace level.
     *
     * @param ctx the channel context.
     * @param keepAliveResponse the keep alive request that was sent when keep alive was triggered
     */
    protected void onKeepAliveResponse(ChannelHandlerContext ctx, CouchbaseResponse keepAliveResponse) {
        if (traceEnabled) {
            LOGGER.trace(logIdent(ctx, endpoint) + "keepAlive was answered, status "
                    + keepAliveResponse.status());
        }
    }

    /**
     * Returns the current request if set.
     *
     * @return the current request.
     */
    protected REQUEST currentRequest() {
        return currentRequest;
    }

    /**
     * Sets current request.
     *
     * FIXME this is temporary solution for {@link com.couchbase.client.core.endpoint.dcp.DCPHandler}
     * @param request request to become the current one
     */
    protected void currentRequest(REQUEST request) {
        currentRequest = request;
    }

    /**
     * @return stringified version of the remote node's hostname
     */
    protected String remoteHostname() {
        return remoteHostname;
    }

    /**
     * Returns environment.
     *
     * @return the environment
     */
    protected CoreEnvironment env() {
        return endpoint.environment();
    }

    /**
     * The parent endpoint.
     */
    protected AbstractEndpoint endpoint() {
        return endpoint;
    }

    /**
     * Simple log helper to give logs a common prefix.
     *
     * @param ctx the context.
     * @param endpoint the endpoint.
     * @return a prefix string for logs.
     */
    protected static String logIdent(final ChannelHandlerContext ctx, final Endpoint endpoint) {
        return "[" + ctx.channel().remoteAddress() + "][" + endpoint.getClass().getSimpleName() + "]: ";
    }

    private class KeepAliveResponseAction implements Action1<CouchbaseResponse> {
        private final ChannelHandlerContext ctx;
        public KeepAliveResponseAction(ChannelHandlerContext ctx) { this.ctx = ctx; }

        @Override
        public void call(CouchbaseResponse couchbaseResponse) {
            onKeepAliveResponse(this.ctx, couchbaseResponse);
        }
    }

    /**
     * Add basic authentication headers to a {@link HttpRequest}.
     *
     * The given information is Base64 encoded and the authorization header is set appropriately. Since this needs
     * to be done for every request, it is refactored out.
     *
     * @param ctx the handler context.
     * @param request the request where the header should be added.
     * @param user the username for auth.
     * @param password the password for auth.
     */
    public static void addHttpBasicAuth(final ChannelHandlerContext ctx, final HttpRequest request, final String user,
        final String password) {
        final String pw = password == null ? "" : password;

        ByteBuf raw = ctx.alloc().buffer(user.length() + pw.length() + 1);
        raw.writeBytes((user + ":" + pw).getBytes(CHARSET));
        ByteBuf encoded = Base64.encode(raw, false);
        request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded.toString(CHARSET));
        encoded.release();
        raw.release();
    }

    /**
     * Helper method to return the remote http host, cached.
     *
     * @param ctx the handler context.
     * @return the remote http host.
     */
    protected String remoteHttpHost(ChannelHandlerContext ctx) {
        if (remoteHttpHost == null) {
            SocketAddress addr = ctx.channel().remoteAddress();
            if (addr instanceof InetSocketAddress) {
                InetSocketAddress inetAddr = (InetSocketAddress) addr;
                remoteHttpHost = inetAddr.getAddress().getHostAddress() + ":" + inetAddr.getPort();
            } else {
                remoteHttpHost = addr.toString();
            }
        }
        return remoteHttpHost;
    }

}
