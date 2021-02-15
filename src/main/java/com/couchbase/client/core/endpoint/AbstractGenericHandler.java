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
package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.ResponseHandler;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.CoreScheduler;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.logging.RedactableArgument;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.DiagnosticRequest;
import com.couchbase.client.core.message.KeepAlive;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.metrics.NetworkLatencyMetricsIdentifier;
import com.couchbase.client.core.retry.RetryHelper;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.tracing.ThresholdLogReporter;
import com.couchbase.client.core.tracing.ThresholdLogSpan;
import com.lmax.disruptor.EventSink;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.ScheduledFuture;
import io.opentracing.Scope;
import io.opentracing.Span;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action0;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.couchbase.client.core.endpoint.kv.KeyValueFeatureHandler.paddedHex;
import static com.couchbase.client.core.logging.RedactableArgument.system;
import static com.couchbase.client.core.logging.RedactableArgument.user;
import static com.couchbase.client.core.utils.Observables.failSafe;

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
     * Contains all the outstanding dispatch spans.
     */
    private final Queue<Span> dispatchSpans;

    /**
     * Holds the current dispatch span during the decode phase.
     */
    private Span currentDispatchSpan;

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
     * Remote socket, stringified with host and port.
     */
    private String remoteSocket;

    /**
     * Local socket, stringified with host and port.
     */
    private String localSocket;

    /**
     * Local id combination of cluster ID and channel.
     */
    private String localId;

    /**
     * The future which is used to eventually signal a connected channel.
     */
    private ChannelPromise connectFuture;

    /**
     * Returns the remote http host in usable format.
     */
    private String remoteHttpHost;

    private final int sentQueueLimit;

    private final boolean pipeline;

    private volatile long failedKeepAliveRequests;

    /**
     * If continuous keepalive is enabled, holds the future for continuous execution.
     *
     * This is important since it needs to be cancelled once the channel goes out
     * of scope/inactive.
     */
    private volatile ScheduledFuture<?> continuousKeepAliveFuture;

    /**
     * Creates a new {@link AbstractGenericHandler} with the default queue.
     *
     * @param endpoint the endpoint reference.
     * @param responseBuffer the response buffer.
     */
    protected AbstractGenericHandler(final AbstractEndpoint endpoint, final EventSink<ResponseEvent> responseBuffer, final boolean isTransient, final boolean pipeline) {
        this(endpoint, responseBuffer, new ArrayDeque<REQUEST>(), isTransient, pipeline);
    }

    /**
     * Creates a new {@link AbstractGenericHandler} with a custom queue.
     *
     * @param endpoint the endpoint reference.
     * @param responseBuffer the response buffer.
     * @param queue the queue.
     */
    protected AbstractGenericHandler(final AbstractEndpoint endpoint, final EventSink<ResponseEvent> responseBuffer,
        final Queue<REQUEST> queue, final boolean isTransient, final boolean pipeline) {
        this.pipeline = pipeline;
        this.endpoint = endpoint;
        this.responseBuffer = responseBuffer;
        this.sentRequestQueue = queue;
        this.currentDecodingState = DecodingState.INITIAL;
        this.isTransient = isTransient;
        this.traceEnabled = LOGGER.isTraceEnabled();
        this.sentRequestTimings = new ArrayDeque<Long>();
        this.dispatchSpans = new ArrayDeque<Span>();
        this.classNameCache = new IdentityHashMap<Class<? extends CouchbaseRequest>, String>();
        this.moveResponseOut = env() == null || !env().callbacksOnIoPool();
        this.sentQueueLimit = Integer.parseInt(System.getProperty("com.couchbase.sentRequestQueueLimit", "5120"));
        this.failedKeepAliveRequests = 0;
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
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (!pipeline && !(msg instanceof KeepAlive) && (!sentRequestQueue.isEmpty() || currentDecodingState != DecodingState.INITIAL)) {
            if (traceEnabled) {
                LOGGER.trace("Rescheduling {} because pipelining disable and a request is in-flight.", msg);
            }
            RetryHelper.retryOrCancel(env(), (CouchbaseRequest) msg, responseBuffer);
            return;
        }

        if (sentRequestQueue.size() < sentQueueLimit) {
            super.write(ctx, msg, promise);
        } else {
            LOGGER.warn("Rescheduling {} because sentRequestQueueLimit reached.", msg);
            RetryHelper.retryOrCancel(env(), (CouchbaseRequest) msg, responseBuffer);
        }
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, REQUEST msg, List<Object> out) throws Exception {
        ENCODED request;
        try {
            request = encodeRequest(ctx, msg);
        } catch (Exception ex) {
            msg.observable().onError(new RequestCancelledException("Error while encoding Request, cancelling.", ex));
            // we need to re-throw the error because netty expects either an exception
            // or at least one message encoded. just returning won't work
            throw ex;
        }
        sentRequestQueue.offer(msg);
        out.add(request);
        sentRequestTimings.offer(System.nanoTime());

        if (localId == null) {
            populateInfo(ctx);
        }

        msg.lastLocalSocket(localSocket);
        msg.lastRemoteSocket(remoteSocket);
        msg.lastLocalId(localId);

        if (env().operationTracingEnabled() && msg.span() != null) {
            Scope scope = env().tracer()
                .buildSpan("dispatch_to_server")
                .asChildOf(msg.span())
                .withTag("peer.address", remoteSocket)
                .withTag("local.address", localSocket)
                .withTag("local.id", localId)
                .startActive(false);
            dispatchSpans.offer(scope.span());
            scope.close();
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, RESPONSE msg, List<Object> out) throws Exception {
        if (currentDecodingState == DecodingState.INITIAL) {
            initialDecodeTasks(ctx);
        }

        try {
            CouchbaseResponse response = decodeResponse(ctx, msg);
            if (response != null) {
                if (currentRequest instanceof DiagnosticRequest) {
                    ((DiagnosticRequest) currentRequest).localSocket(ctx.channel().localAddress());
                    ((DiagnosticRequest) currentRequest).remoteSocket(ctx.channel().remoteAddress());
                }

                if (currentDispatchSpan != null) {
                    env().tracer().scopeManager()
                        .activate(currentDispatchSpan, true)
                        .close();
                    if (currentDispatchSpan instanceof ThresholdLogSpan) {
                        currentDispatchSpan.setBaggageItem(
                            ThresholdLogReporter.KEY_DISPATCH_MICROS,
                            Long.toString(((ThresholdLogSpan) currentDispatchSpan).durationMicros())
                        );
                    }
                    currentDispatchSpan = null;
                }

                publishResponse(response, currentRequest.observable());
                if (currentDecodingState == DecodingState.FINISHED) {
                    writeMetrics(response);
                    if (currentRequest instanceof KeepAlive) {
                        endpoint.setLastKeepAliveLatency(currentOpTime);
                    }
                }
            }
        } catch (CouchbaseException e) {
            failSafe(env().scheduler(), moveResponseOut, currentRequest.observable(), e);
        } catch (Exception e) {
            failSafe(env().scheduler(), moveResponseOut, currentRequest.observable(), new CouchbaseException(e));
        }

        if (currentDecodingState == DecodingState.FINISHED) {
            endpoint.notifyResponseDecoded(currentRequest instanceof KeepAlive);
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
                LOGGER.warn("Could not collect latency metric for request {} ({})",
                    user(currentRequest.toString()),
                    currentOpTime,
                    e
                );
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

    protected Span currentDispatchSpan() {
        return currentDispatchSpan;
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

        if (env().operationTracingEnabled()) {
            Span dispatchSpan = dispatchSpans.poll();
            if (dispatchSpan != null) {
                currentDispatchSpan = dispatchSpan;
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
    private void completeResponse(final CouchbaseResponse response,
        final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        // Noone is listening anymore, handle tracing and/or orphan reporting
        // depending on if enabled or not.
        CouchbaseRequest request = response.request();
        if (request != null && !request.isActive()) {
            if (env().operationTracingEnabled() && request.span() != null) {
                Scope scope = env().tracer().scopeManager()
                        .activate(request.span(), true);
                scope.span().setBaggageItem("couchbase.orphan", "true");
                scope.close();
            }
            if (env().orphanResponseReportingEnabled()) {
                env().orphanResponseReporter().report(response);
            }
        }

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
    private void scheduleDirect(CoreScheduler scheduler, final CouchbaseResponse response,
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
    private void scheduleWorker(Scheduler scheduler, final CouchbaseResponse response,
        final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        final Scheduler.Worker worker = scheduler.createWorker();
        worker.schedule(new Action0() {
            @Override
            public void call() {
                completeResponse(response, observable);
                worker.unsubscribe();
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
        populateInfo(ctx);
        channelActiveSideEffects(ctx);
        ctx.fireChannelActive();
    }

    private void populateInfo(final ChannelHandlerContext ctx) {
        try {
            localId = paddedHex(endpoint.context().coreId()) + "/" + paddedHex(ctx.channel().hashCode());
        } catch (Exception ex) {
            // can happen during testing in some cases
            LOGGER.info("Could not define channel ID, ignoring.");
            localId = paddedHex(0) + "/" + paddedHex(0);
        }
        SocketAddress addr = ctx.channel().remoteAddress();
        if (addr instanceof InetSocketAddress) {
            // Avoid lookup, so just use the address
            InetSocketAddress ia = ((InetSocketAddress) addr);
            remoteHostname = ia.getAddress().getHostAddress();
            remoteSocket = remoteHostname + ":" + ia.getPort();
        } else {
            // Should not happen in production, but in testing it might be different
            remoteHostname = addr.toString();
            remoteSocket = addr.toString();
        }

        SocketAddress localAddr = ctx.channel().localAddress();
        if (localAddr instanceof InetSocketAddress) {
            // Avoid lookup, so just use the address
            InetSocketAddress ia = ((InetSocketAddress) localAddr);
            localSocket = ia.getAddress().getHostAddress() + ":" + ia.getPort();
        } else {
            localSocket = localAddr.toString();
        }

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

    /**
     * Helper method to perform certain side effects when the channel is connected.
     */
    private void channelActiveSideEffects(final ChannelHandlerContext ctx) {
        long interval = env().keepAliveInterval();
        if (env().continuousKeepAliveEnabled()) {
            continuousKeepAliveFuture = ctx.executor().scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    if (shouldSendKeepAlive()) {
                        createAndWriteKeepAlive(ctx);
                    }
                }
            }, interval, interval, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(logIdent(ctx, endpoint) + "Connection reset by peer: " + cause.getMessage(), cause);
            } else {
                LOGGER.info("{}Connection reset by peer: {}", logIdent(ctx, endpoint), cause.getMessage());
            }
            handleOutstandingOperations(ctx);
        } else if (cause instanceof DecoderException && cause.getCause() instanceof SSLHandshakeException) {
            if (!connectFuture.isDone()) {
                connectFuture.setFailure(cause.getCause());
            } else {
                // This should not be possible, since handshake is done before connecting. But just in case, we
                // can trap and log an error that might slip through for one reason or another.
                LOGGER.warn("{}Caught SSL exception after being connected: {}", logIdent(ctx, endpoint),
                    cause.getMessage(), cause);
            }
        } else {
            LOGGER.warn("{}Caught unknown exception: {}", logIdent(ctx, endpoint), cause.getMessage(), cause);
            ctx.fireExceptionCaught(cause);
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        if (continuousKeepAliveFuture != null) {
            // cancel the continuous execution and interrupt a job.
            LOGGER.trace("Stopping continuous keepalive execution");
            continuousKeepAliveFuture.cancel(true);
            continuousKeepAliveFuture = null;
        }
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
                failSafe(env().scheduler(), moveResponseOut, req.observable(),
                        new RequestCancelledException("Request cancelled in-flight."));
            } catch (Exception ex) {
                LOGGER.info(
                    "Exception thrown while cancelling outstanding operation: {}",
                    user(req.toString()),
                    ex
                );
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
            if (!shouldSendKeepAlive() || env().continuousKeepAliveEnabled()) {
                return;
            }
            createAndWriteKeepAlive(ctx);
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    /**
     * Helper method to create, write and flush the keepalive message.
     */
    private void createAndWriteKeepAlive(final ChannelHandlerContext ctx) {
        final CouchbaseRequest keepAlive = createKeepAliveRequest();
        if (keepAlive != null) {
            Subscriber<CouchbaseResponse> subscriber = new KeepAliveResponseAction(ctx);
            keepAlive.subscriber(subscriber);
            keepAlive
                .observable()
                .timeout(env().keepAliveTimeout(), TimeUnit.MILLISECONDS)
                .subscribe(subscriber);

            onKeepAliveFired(ctx, keepAlive);

            Channel channel = ctx.channel();
            if (channel.isActive() && channel.isWritable()) {
                ctx.pipeline().writeAndFlush(keepAlive);
            }
        }
    }

    /**
     * Returns true if there is at least one active request in the queue.
     */
    private boolean atLeastOneActiveInRequestQueue() {
        for (REQUEST elem : sentRequestQueue) {
            if (elem.isActive()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Helper method to check if conditions are met to send a keepalive right now.
     *
     * @return true if keepalive can be sent, false otherwise.
     */
    public boolean shouldSendKeepAlive() {
        if (pipeline) {
            return true; // always send if pipelining is enabled
        }
        return (sentRequestQueue.isEmpty() || !atLeastOneActiveInRequestQueue()) && currentDecodingState == DecodingState.INITIAL;
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
        if (env().continuousKeepAliveEnabled() && LOGGER.isTraceEnabled()) {
            LOGGER.trace(logIdent(ctx, endpoint) + "Continuous KeepAlive fired");
        } else if (LOGGER.isDebugEnabled()) {
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
    protected static RedactableArgument logIdent(final ChannelHandlerContext ctx, final Endpoint endpoint) {
        return system("[" + ctx.channel().remoteAddress() + "][" + endpoint.getClass().getSimpleName() + "]: ");
    }

    private class KeepAliveResponseAction extends Subscriber<CouchbaseResponse> {
        private final ChannelHandlerContext ctx;
        KeepAliveResponseAction(ChannelHandlerContext ctx) { this.ctx = ctx; }

        @Override
        public void onCompleted() {
            failedKeepAliveRequests = 0;
        }

        @Override
        public void onError(Throwable e) {
            if (ctx.channel() == null || !ctx.channel().isActive()) {
                return;
            }

            failedKeepAliveRequests++;

            if (e instanceof TimeoutException) {
                LOGGER.warn("{}KeepAliveRequest timed out after {} ms. This was attempt #{} of {}.",
                    logIdent(ctx, endpoint), env().keepAliveTimeout(), failedKeepAliveRequests, env().keepAliveErrorThreshold());

                endpoint.setLastKeepAliveLatency(TimeUnit.MILLISECONDS.toMicros(env().keepAliveTimeout()));
            } else {
                LOGGER.warn("{}Got error while consuming KeepAliveResponse. This was attempt #{} of {}.",
                    logIdent(ctx, endpoint), failedKeepAliveRequests, env().keepAliveErrorThreshold(), e);
            }

            if (failedKeepAliveRequests >= env().keepAliveErrorThreshold()) {
                LOGGER.warn( "{}Failed to receive a KeepAliveResponse from the server after {} attempts." +
                        " This may indicate the connection is dead. Closing this socket proactively.",
                    system(logIdent(ctx, endpoint)), env().keepAliveErrorThreshold());
                ctx.close().addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            LOGGER.warn("Error while proactively closing the socket.", future.cause());
                        }
                    }
                });
            }
        }

        @Override
        public void onNext(CouchbaseResponse couchbaseResponse) {
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

        // if both user and password are null or empty, don't add http basic auth
        // this is usually the case when certificate auth is used.
        if ((user == null || user.isEmpty()) && (password == null || password.isEmpty())) {
            return;
        }

        final String pw = password == null ? "" : password;

        ByteBuf raw = ctx.alloc().buffer(user.length() + pw.length() + 1);
        raw.writeBytes((user + ":" + pw).getBytes(CHARSET));
        ByteBuf encoded = Base64.encode(raw, false);
        request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded.toString(CHARSET));
        encoded.release();
        raw.release();
    }

    /**
     * Helper method to complete the request span, called from child instances.
     *
     * @param request the corresponding request.
     */
    protected void completeRequestSpan(final CouchbaseRequest request) {
        if (request != null && request.span() != null) {
            if (env().operationTracingEnabled()) {
                env().tracer().scopeManager()
                    .activate(request.span(), true)
                    .close();
            }
        }
    }

    /**
     * Helper method to return the remote http host, cached.
     *
     * @param ctx the handler context.
     * @return the remote http host.
     */
    protected String remoteHttpHost(ChannelHandlerContext ctx) {
        if (remoteHttpHost == null) {
            if (endpoint.remoteAddress() != null) {
                remoteHttpHost = endpoint.remoteAddress();
            } else {
                SocketAddress addr = ctx.channel().remoteAddress();
                if (addr instanceof InetSocketAddress) {
                    InetSocketAddress inetAddr = (InetSocketAddress) addr;
                    remoteHttpHost = inetAddr.getAddress().getHostAddress() + ":" + inetAddr.getPort();
                } else {
                    remoteHttpHost = addr.toString();
                }
            }
        }
        return remoteHttpHost;
    }

    public DecodingState getDecodingState() {
        return this.currentDecodingState;
    }

}
