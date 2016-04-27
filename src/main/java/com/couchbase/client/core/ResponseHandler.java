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
package com.couchbase.client.core;

import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.internal.SignalConfigReload;
import com.couchbase.client.core.message.kv.BinaryResponse;
import com.couchbase.client.core.time.Delay;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorTwoArg;
import io.netty.util.CharsetUtil;
import rx.Scheduler;
import rx.functions.Action0;
import rx.subjects.Subject;

import java.util.concurrent.TimeUnit;

public class ResponseHandler implements EventHandler<ResponseEvent> {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ResponseHandler.class);

    private final ClusterFacade cluster;
    private final ConfigurationProvider configurationProvider;
    private final CoreEnvironment environment;
    private final boolean traceLoggingEnabled;

    /**
     * Creates a new {@link ResponseHandler}.
     *
     * @param environment the global environment.
     * @param cluster the cluster reference.
     * @param provider th configuration provider.
     */
    public ResponseHandler(CoreEnvironment environment, ClusterFacade cluster, ConfigurationProvider provider) {
        this.cluster = cluster;
        this.configurationProvider = provider;
        this.environment = environment;
        traceLoggingEnabled = LOGGER.isTraceEnabled();
    }

    /**
     * Translates {@link CouchbaseRequest}s into {@link RequestEvent}s.
     */
    public static final EventTranslatorTwoArg<ResponseEvent, CouchbaseMessage,
        Subject<CouchbaseResponse, CouchbaseResponse>> RESPONSE_TRANSLATOR =
        new EventTranslatorTwoArg<ResponseEvent, CouchbaseMessage, Subject<CouchbaseResponse, CouchbaseResponse>>() {
            @Override
            public void translateTo(ResponseEvent event, long sequence, CouchbaseMessage message,
                Subject<CouchbaseResponse, CouchbaseResponse> observable) {
                event.setMessage(message);
                event.setObservable(observable);
            }
        };

    /**
     * Handles {@link ResponseEvent}s that come into the response RingBuffer.
     *
     * Hey I just mapped you,
     * And this is crazy,
     * But here's my data
     * so subscribe me maybe.
     *
     * It's hard to block right,
     * at you baby,
     * But here's my data ,
     * so subscribe me maybe.
     */
    @Override
    public void onEvent(final ResponseEvent event, long sequence, boolean endOfBatch) throws Exception {
        try {
            CouchbaseMessage message = event.getMessage();
            if (message instanceof SignalConfigReload) {
                configurationProvider.signalOutdated();
            } else if (message instanceof CouchbaseResponse) {
                final CouchbaseResponse response = (CouchbaseResponse) message;
                ResponseStatus status = response.status();
                if (status == ResponseStatus.RETRY) {
                    retry(event);
                } else {
                    final Scheduler.Worker worker = environment.scheduler().createWorker();
                    final Subject<CouchbaseResponse, CouchbaseResponse> obs = event.getObservable();
                    worker.schedule(new Action0() {
                        @Override
                        public void call() {
                            try {
                                obs.onNext(response);
                                obs.onCompleted();
                            } catch(Exception ex) {
                                obs.onError(ex);
                            } finally {
                                worker.unsubscribe();
                            }
                        }
                    });
                }
            } else if (message instanceof CouchbaseRequest) {
                retry(event);
            } else {
                throw new IllegalStateException("Got message type I do not understand: " + message);
            }
        } finally {
           event.setMessage(null);
           event.setObservable(null);
        }
    }

    private void retry(final ResponseEvent event) {
        final CouchbaseMessage message = event.getMessage();
        if (message instanceof CouchbaseRequest) {
            scheduleForRetry((CouchbaseRequest) message);
        } else {

            CouchbaseRequest request = ((CouchbaseResponse) message).request();
            if (request != null) {
                scheduleForRetry(request);
            } else {
                event.getObservable().onError(new CouchbaseException("Operation failed because it does not "
                    + "support cloning."));
            }
            if (message instanceof BinaryResponse) {
                BinaryResponse response = (BinaryResponse) message;
                if (response.content() != null && response.content().readableBytes() > 0) {
                    try {
                        String config = response.content().toString(CharsetUtil.UTF_8).trim();
                        if (config.startsWith("{")) {
                            configurationProvider.proposeBucketConfig(response.bucket(), config);
                        }
                    } finally {
                        response.content().release();
                    }
                }
            }
        }
    }

    /**
     * Helper method which schedules the given {@link CouchbaseRequest} with a delay for further retry.
     *
     * @param request the request to retry.
     */
    private void scheduleForRetry(final CouchbaseRequest request) {
        CoreEnvironment env = environment;
        Delay delay = env.retryDelay();

        long delayTime = delay.calculate(request.incrementRetryCount());
        TimeUnit delayUnit = delay.unit();

        if (traceLoggingEnabled) {
            LOGGER.trace("Retrying {} with a delay of {} {}", request, delayTime, delayUnit);
        }

        final Scheduler.Worker worker = env.scheduler().createWorker();
        worker.schedule(new Action0() {
            @Override
            public void call() {
                try {
                    cluster.send(request);
                } finally {
                    worker.unsubscribe();
                }
            }
        }, delayTime, delayUnit);
    }
}
