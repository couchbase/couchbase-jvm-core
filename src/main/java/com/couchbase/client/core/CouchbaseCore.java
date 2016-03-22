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

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.DefaultConfigurationProvider;
import com.couchbase.client.core.endpoint.dcp.DCPConnection;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.env.Diagnostics;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.CloseBucketRequest;
import com.couchbase.client.core.message.cluster.CloseBucketResponse;
import com.couchbase.client.core.message.cluster.ClusterRequest;
import com.couchbase.client.core.message.cluster.DisconnectRequest;
import com.couchbase.client.core.message.cluster.DisconnectResponse;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionResponse;
import com.couchbase.client.core.message.internal.AddNodeRequest;
import com.couchbase.client.core.message.internal.AddNodeResponse;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.AddServiceResponse;
import com.couchbase.client.core.message.internal.GetConfigProviderRequest;
import com.couchbase.client.core.message.internal.GetConfigProviderResponse;
import com.couchbase.client.core.message.internal.InternalRequest;
import com.couchbase.client.core.message.internal.RemoveNodeRequest;
import com.couchbase.client.core.message.internal.RemoveNodeResponse;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.message.internal.RemoveServiceResponse;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.util.concurrent.DefaultThreadFactory;
import rx.Observable;
import rx.functions.Func1;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The general implementation of a {@link ClusterFacade}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class CouchbaseCore implements ClusterFacade {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CouchbaseCore.class);

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
     * The handler for all cluster nodes.
     */
    private final RequestHandler requestHandler;

    /**
     * The configuration provider in use.
     */
    private final ConfigurationProvider configProvider;

    private final CoreEnvironment environment;

    private final Disruptor<RequestEvent> requestDisruptor;
    private final Disruptor<ResponseEvent> responseDisruptor;
    private final ExecutorService disruptorExecutor;

    private volatile boolean sharedEnvironment = true;

    /**
     * Populate the static exceptions with stack trace elements.
     */
    static {
        BACKPRESSURE_EXCEPTION.setStackTrace(new StackTraceElement[0]);
    }

    /**
     * Creates a new {@link CouchbaseCore}.
     */
    public CouchbaseCore() {
        this(DefaultCoreEnvironment.create());
        sharedEnvironment = false;
    }

    /**
     * Creates a new {@link CouchbaseCore}.
     */
    public CouchbaseCore(final CoreEnvironment environment) {
        LOGGER.info(environment.toString());
        LOGGER.debug(Diagnostics.collectAndFormat());

        this.environment = environment;
        configProvider = new DefaultConfigurationProvider(this, environment);
        disruptorExecutor = Executors.newFixedThreadPool(2, new DefaultThreadFactory("cb-core", true));

        responseDisruptor = new Disruptor<ResponseEvent>(
            new ResponseEventFactory(),
            environment.responseBufferSize(),
            disruptorExecutor
        );
        responseDisruptor.handleExceptionsWith(new ExceptionHandler<ResponseEvent>() {
            @Override
            public void handleEventException(Throwable ex, long sequence, ResponseEvent event) {
                LOGGER.warn("Exception while Handling Response Events {}", event, ex);
            }

            @Override
            public void handleOnStartException(Throwable ex) {
                LOGGER.warn("Exception while Starting Response RingBuffer", ex);
            }

            @Override
            public void handleOnShutdownException(Throwable ex) {
                LOGGER.info("Exception while shutting down Response RingBuffer", ex);
            }
        });
        responseDisruptor.handleEventsWith(new ResponseHandler(environment, this, configProvider));
        responseDisruptor.start();
        RingBuffer<ResponseEvent> responseRingBuffer = responseDisruptor.getRingBuffer();

        requestDisruptor = new Disruptor<RequestEvent>(
            new RequestEventFactory(),
            environment.requestBufferSize(),
            disruptorExecutor
        );
        requestHandler = new RequestHandler(environment, configProvider.configs(), responseRingBuffer);
        requestDisruptor.handleExceptionsWith(new ExceptionHandler<RequestEvent>() {
            @Override
            public void handleEventException(Throwable ex, long sequence, RequestEvent event) {
                LOGGER.warn("Exception while Handling Request Events {}", event, ex);
            }

            @Override
            public void handleOnStartException(Throwable ex) {
                LOGGER.warn("Exception while Starting Request RingBuffer", ex);
            }

            @Override
            public void handleOnShutdownException(Throwable ex) {
                LOGGER.info("Exception while shutting down Request RingBuffer", ex);
            }
        });
        requestDisruptor.handleEventsWith(requestHandler);
        requestDisruptor.start();
        requestRingBuffer = requestDisruptor.getRingBuffer();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R extends CouchbaseResponse> Observable<R> send(CouchbaseRequest request) {
        if (request instanceof InternalRequest) {
            handleInternalRequest(request);
            return (Observable<R>) request.observable().observeOn(environment.scheduler());
        } else if (request instanceof ClusterRequest) {
            handleClusterRequest(request);
            return (Observable<R>) request.observable().observeOn(environment.scheduler());
        } else {
            boolean published = requestRingBuffer.tryPublishEvent(REQUEST_TRANSLATOR, request);
            if (!published) {
                request.observable().onError(BACKPRESSURE_EXCEPTION);
            }
            return (Observable<R>) request.observable();
        }
    }

    /**
     * Helper method to handle the cluster requests.
     *
     * @param request the request to dispatch.
     */
    private void handleClusterRequest(final CouchbaseRequest request) {
        if (request instanceof SeedNodesRequest) {
            boolean success = configProvider.seedHosts(((SeedNodesRequest) request).nodes(), true);
            ResponseStatus status = success ? ResponseStatus.SUCCESS : ResponseStatus.FAILURE;
            request.observable().onNext(new SeedNodesResponse(status));
            request.observable().onCompleted();
        } else if (request instanceof OpenBucketRequest) {
            configProvider
                .openBucket(request.bucket(), request.password())
                .flatMap(new Func1<ClusterConfig, Observable<ClusterConfig>>() {
                    @Override
                    public Observable<ClusterConfig> call(ClusterConfig clusterConfig) {
                        return requestHandler.reconfigure(clusterConfig);
                    }
                })
                .map(new Func1<ClusterConfig, OpenBucketResponse>() {
                    @Override
                    public OpenBucketResponse call(final ClusterConfig clusterConfig) {
                        if (clusterConfig.hasBucket(request.bucket())) {
                            return new OpenBucketResponse(ResponseStatus.SUCCESS);
                        }
                        throw new CouchbaseException("Could not open bucket.");
                    }
                })
                .subscribe(request.observable());
        } else if (request instanceof CloseBucketRequest) {
            configProvider
                .closeBucket(request.bucket())
                .flatMap(new Func1<ClusterConfig, Observable<ClusterConfig>>() {
                    @Override
                    public Observable<ClusterConfig> call(ClusterConfig clusterConfig) {
                        return requestHandler.reconfigure(clusterConfig);
                    }
                })
                .map(new Func1<ClusterConfig, CloseBucketResponse>() {
                    @Override
                    public CloseBucketResponse call(ClusterConfig clusterConfig) {
                        if (!clusterConfig.hasBucket(request.bucket())) {
                            return new CloseBucketResponse(ResponseStatus.SUCCESS);
                        }
                        throw new CouchbaseException("Could not close bucket.");
                    }
                })
                .subscribe(request.observable());
        } else if (request instanceof DisconnectRequest) {
            configProvider
                .closeBuckets()
                .flatMap(new Func1<Boolean, Observable<Boolean>>() {
                    @Override
                    public Observable<Boolean> call(Boolean done) {
                        return sharedEnvironment ? Observable.just(true) : environment.shutdownAsync();
                    }
                }).map(new Func1<Boolean, Boolean>() {
                    @Override
                    public Boolean call(Boolean success) {
                        requestDisruptor.shutdown();
                        responseDisruptor.shutdown();
                        disruptorExecutor.shutdownNow();
                        return success;
                    }
                })
                .map(new Func1<Boolean, DisconnectResponse>() {
                    @Override
                    public DisconnectResponse call(Boolean success) {
                        return new DisconnectResponse(ResponseStatus.SUCCESS);
                    }
                })
                .subscribe(request.observable());
        } else if (request instanceof GetClusterConfigRequest) {
            request.observable().onNext(new GetClusterConfigResponse(configProvider.config(), ResponseStatus.SUCCESS));
            request.observable().onCompleted();
        } else if (request instanceof OpenConnectionRequest) {
            request.observable().onNext(new OpenConnectionResponse(
                    new DCPConnection(environment, this, request.bucket(), request.password()), ResponseStatus.SUCCESS));
            request.observable().onCompleted();
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
        if (request instanceof GetConfigProviderRequest) {
            request.observable().onNext(new GetConfigProviderResponse(configProvider));
            request.observable().onCompleted();
        } else if (request instanceof AddNodeRequest) {
            requestHandler
                .addNode(((AddNodeRequest) request).hostname())
                .map(new Func1<LifecycleState, AddNodeResponse>() {
                    @Override
                    public AddNodeResponse call(LifecycleState state) {
                        return new AddNodeResponse(ResponseStatus.SUCCESS, ((AddNodeRequest) request).hostname());
                    }
                })
                .subscribe(request.observable());
        } else if (request instanceof RemoveNodeRequest) {
            requestHandler
                .removeNode(((RemoveNodeRequest) request).hostname())
                .map(new Func1<LifecycleState, RemoveNodeResponse>() {
                    @Override
                    public RemoveNodeResponse call(LifecycleState state) {
                        return new RemoveNodeResponse(ResponseStatus.SUCCESS);
                    }
                })
                .subscribe(request.observable());
        } else if (request instanceof AddServiceRequest) {
            requestHandler
                .addService((AddServiceRequest) request)
                .map(new Func1<Service, AddServiceResponse>() {
                    @Override
                    public AddServiceResponse call(Service service) {
                        return new AddServiceResponse(ResponseStatus.SUCCESS, ((AddServiceRequest) request).hostname());
                    }
                })
                .subscribe(request.observable());
        } else if (request instanceof RemoveServiceRequest) {
            requestHandler
                .removeService((RemoveServiceRequest) request)
                .map(new Func1<Service, RemoveServiceResponse>() {
                    @Override
                    public RemoveServiceResponse call(Service service) {
                        return new RemoveServiceResponse(ResponseStatus.SUCCESS);
                    }
                })
                .subscribe(request.observable());
        } else {
            request
                .observable()
                .onError(new IllegalArgumentException("Unknown request " + request));
        }
    }
}
