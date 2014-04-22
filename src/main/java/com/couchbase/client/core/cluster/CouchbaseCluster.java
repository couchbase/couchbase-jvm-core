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
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.DefaultConfigurationProvider;
import com.couchbase.client.core.env.CouchbaseEnvironment;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.cluster.ClusterRequest;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.message.internal.AddNodeRequest;
import com.couchbase.client.core.message.internal.AddNodeResponse;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.AddServiceResponse;
import com.couchbase.client.core.message.internal.InternalRequest;
import com.couchbase.client.core.message.internal.RemoveNodeRequest;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import rx.Observable;
import rx.Observer;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * The general implementation of a {@link Cluster}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class CouchbaseCluster implements Cluster {

    /**
     * Translates {@link CouchbaseRequest}s into {@link RequestEvent}s.
     */
    private static final EventTranslatorOneArg<RequestEvent, CouchbaseRequest> REQUEST_TRANSLATOR =
        new EventTranslatorOneArg<RequestEvent, CouchbaseRequest>() {
            @Override
            public void translateTo(RequestEvent event, long sequence, CouchbaseRequest request) {
                event.setRequest(request);
            }
        };

    /**
     * A preconstructed {@link BackpressureException}.
     */
    private static final BackpressureException BACKPRESSURE_EXCEPTION = new BackpressureException();

    /**
     * The {@link RequestEvent} {@link RingBuffer}.
     */
    private final RingBuffer<RequestEvent> requestRingBuffer;

    /**
     * The {@link ResponseEvent} {@link RingBuffer}.
     */
    private final RingBuffer<ResponseEvent> responseRingBuffer;

    /**
     * The handler for all cluster nodes.
     */
    private final RequestHandler requestHandler;

    private final ConfigurationProvider configProvider;


    /**
     * Populate the static exceptions with stack trace elements.
     */
    static {
        BACKPRESSURE_EXCEPTION.setStackTrace(new StackTraceElement[0]);
    }

    /**
     * Creates a new {@link CouchbaseCluster}.
     */
    public CouchbaseCluster() {
        this(new CouchbaseEnvironment());
    }

    /**
     * Creates a new {@link CouchbaseCluster}.
     */
    public CouchbaseCluster(Environment environment) {
        configProvider = new DefaultConfigurationProvider(this);
        Executor executor = Executors.newFixedThreadPool(2);

        Disruptor<ResponseEvent> responseDisruptor = new Disruptor<ResponseEvent>(
            new ResponseEventFactory(),
            environment.responseBufferSize(),
            executor
        );
        responseDisruptor.handleEventsWith(new ResponseHandler());
        responseDisruptor.start();
        responseRingBuffer = responseDisruptor.getRingBuffer();

        Disruptor<RequestEvent> requestDisruptor = new Disruptor<RequestEvent>(
            new RequestEventFactory(),
            environment.requestBufferSize(),
            executor
        );
        requestHandler = new RequestHandler(environment, configProvider.configs(), responseRingBuffer);
        requestDisruptor.handleEventsWith(requestHandler);
        requestDisruptor.start();
        requestRingBuffer = requestDisruptor.getRingBuffer();
    }

    @Override
    public Observable<CouchbaseResponse> send(final CouchbaseRequest request) {
        final Subject<CouchbaseResponse, CouchbaseResponse> observable = AsyncSubject.create();
        request.observable(observable);

        if (request instanceof InternalRequest) {
            handleInternalRequest(request);
        } else if (request instanceof ClusterRequest) {
            handleClusterRequest(request);
        } else {
            boolean published = requestRingBuffer.tryPublishEvent(REQUEST_TRANSLATOR, request);
            if (!published) {
                observable.onError(BACKPRESSURE_EXCEPTION);
            }
        }

        return observable;
    }

    private void handleClusterRequest(final CouchbaseRequest request) {
        if (request instanceof SeedNodesRequest) {
            boolean success = configProvider.seedHosts(((SeedNodesRequest) request).nodes());
            request.observable().onNext(new SeedNodesResponse(success));
            request.observable().onCompleted();
        } else if (request instanceof OpenBucketRequest) {
            configProvider.openBucket(request.bucket(), ((OpenBucketRequest) request).password()).subscribe(
                new Observer<ClusterConfig>() {
                    @Override
                    public void onCompleted() {
                        request.observable().onCompleted();
                    }

                    @Override
                    public void onError(Throwable e) {
                        request.observable().onError(e);
                    }

                    @Override
                    public void onNext(ClusterConfig clusterConfig) {
                        request.observable().onNext(new OpenBucketResponse());
                    }
                }
            );
        }
    }

    /**
     * Helper method to dispatch internal requests accordingly, without going to the {@link Disruptor}.
     *
     * This makes sure that certain prioritized requests (adding/removing services/nodes) gets done, even when the
     * {@link RingBuffer} is swamped with requests during failure scenarios or high load.
     *
     * @param request the request to dispatch.
     */
    private void handleInternalRequest(final CouchbaseRequest request) {
        if (request instanceof AddNodeRequest) {
            requestHandler.addNode(((AddNodeRequest) request).hostname()).subscribe(new Observer<LifecycleState>() {
                @Override
                public void onCompleted() {
                    request.observable().onCompleted();
                }

                @Override
                public void onError(Throwable e) {
                    request.observable().onError(e);
                }

                @Override
                public void onNext(LifecycleState lifecycleState) {
                    // TODO: its a hack.
                    request.observable().onNext(new AddNodeResponse(((AddNodeRequest) request).hostname()));
                }
            });
        } else if (request instanceof RemoveNodeRequest) {
            requestHandler.removeNode(((RemoveNodeRequest) request).hostname()).subscribe(new Observer<LifecycleState>() {
                @Override
                public void onCompleted() {
                    request.observable().onCompleted();
                }

                @Override
                public void onError(Throwable e) {
                    request.observable().onError(e);
                }

                @Override
                public void onNext(LifecycleState lifecycleState) {
                    // TODO: its a hack.
                    request.observable().onNext(null);
                }
            });
        } else if (request instanceof AddServiceRequest) {
            requestHandler.addService((AddServiceRequest) request).subscribe(new Observer<Service>() {
                @Override
                public void onCompleted() {
                    request.observable().onCompleted();
                }

                @Override
                public void onError(Throwable e) {
                    request.observable().onError(e);
                }

                @Override
                public void onNext(Service service) {

                    request.observable().onNext(new AddServiceResponse(((AddServiceRequest) request).hostname()));
                }
            });
        } else if (request instanceof RemoveServiceRequest) {
            requestHandler.removeService((RemoveServiceRequest) request).subscribe(new Observer<Service>() {
                @Override
                public void onCompleted() {
                    request.observable().onCompleted();
                }

                @Override
                public void onError(Throwable e) {
                    request.observable().onError(e);
                }

                @Override
                public void onNext(Service service) {
                    // TODO: its a hack.
                    request.observable().onNext(null);
                }
            });
        } else {
            request.observable().onError(new IllegalArgumentException("Unknown request " + request));
        }
    }
}
