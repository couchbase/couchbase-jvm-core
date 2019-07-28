/*
 * Copyright (c) 2017 Couchbase, Inc.
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

package com.couchbase.client.core.utils;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.DiagnosticRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.internal.PingReport;
import com.couchbase.client.core.message.internal.PingServiceHealth;
import com.couchbase.client.core.message.kv.NoopRequest;
import com.couchbase.client.core.message.kv.NoopResponse;
import com.couchbase.client.core.service.ServiceType;
import rx.Observable;
import rx.Single;
import rx.functions.Func0;
import rx.functions.Func1;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link HealthPinger} allows to "ping" individual services with
 * real operations for their health.
 *
 * This can be used by up the stack code to assert the given state of
 * a connected cluster and/or bucket.
 *
 * @author Michael Nitschinger
 * @since 1.5.4
 */
public class HealthPinger {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(HealthPinger.class);

    /**
     * Performs a service ping against all or (if given) the services provided.
     *
     * First, all the services are contacted with:
     *
     * - KV - NOOP
     * - N1QL - GET /admin/ping (expect 200 OK)
     * - CBFT - GET /api/ping (expect 200 OK)
     * - Views - GET / (expect 200 OK)
     * - Analytics - POST /query/service with data {"statement": select 1;"} (expect 200 OK)
     *
     * Afterwards, the responses are assembled into a {@link PingReport} and returned.
     *
     * @param env the environment to use.
     * @param bucket the bucket name
     * @param password the password of the bucket
     * @param core the core instance against to check.
     * @param id the id of the ping to use, can be null.
     * @param timeout the timeout for each individual and total ping report.
     * @param timeUnit the time unit for the timeout value.
     * @param types if present, limits the queried services for the given types.
     * @return
     */
    public static Single<PingReport> ping(
        final CoreEnvironment env,
        final String bucket,
        final String password,
        final ClusterFacade core,
        final String id,
        final long timeout,
        final TimeUnit timeUnit,
        ServiceType... types) {

        if (types == null || types.length == 0)  {
            types = new ServiceType[] {
                ServiceType.BINARY,
                ServiceType.ANALYTICS,
                ServiceType.QUERY,
                ServiceType.SEARCH,
                ServiceType.VIEW
            };
        }

        final Set<ServiceType> typeSet = EnumSet.copyOf(Arrays.asList(types));
        return Observable.defer(new Func0<Observable<GetClusterConfigResponse>>() {
            @Override
            public Observable<GetClusterConfigResponse> call() {
                return core.send(new GetClusterConfigRequest());
            }
        })
        .map(new Func1<GetClusterConfigResponse, BucketConfig>() {
            @Override
            public BucketConfig call(GetClusterConfigResponse res) {
                return res.config().bucketConfig(bucket);
            }
        })
        .flatMap(new Func1<BucketConfig, Observable<PingReport>>() {
            @Override
            public Observable<PingReport> call(final BucketConfig config) {
                List<Observable<PingServiceHealth>> services = new ArrayList<Observable<PingServiceHealth>>();
                for (NodeInfo ni : config.nodes()) {
                    for (ServiceType type : ni.services().keySet()) {
                        if (typeSet.contains(type)) {
                            switch (type) {
                                case BINARY:
                                    services.add(pingBinary(ni.hostname(), bucket, core, timeout, timeUnit));
                                    break;
                                case ANALYTICS:
                                    services.add(pingAnalytics(ni.hostname(), bucket, password, core, timeout, timeUnit));
                                    break;
                                case QUERY:
                                    services.add(pingQuery(ni.hostname(), bucket, password, core, timeout, timeUnit));
                                    break;
                                case SEARCH:
                                    services.add(pingSearch(ni.hostname(), bucket, password, core, timeout, timeUnit));
                                    break;
                                case VIEW:
                                    services.add(pingViews(ni.hostname(), bucket, password, core, timeout, timeUnit));
                                    break;
                                default:
                                    throw new IllegalStateException("Unknown type " + type);
                            }
                        }
                    }
                }

                return Observable
                    .merge(services)
                    .toList()
                    .map(new Func1<List<PingServiceHealth>, PingReport>() {
                        @Override
                        public PingReport call(List<PingServiceHealth> services) {
                            return new PingReport(services, env.userAgent(), id, config.rev());
                        }
                    });
            }
        })
        .toSingle();
    }

    /**
     * Pings the service and completes if successful - and fails if it didn't work
     * for some reason (reason is in the exception).
     */
    private static Observable<PingServiceHealth> pingBinary(
        final String hostname, final String bucket,
        final ClusterFacade core, final long timeout, final TimeUnit timeUnit) {
        final AtomicReference<CouchbaseRequest> request = new AtomicReference<CouchbaseRequest>();
        Observable<NoopResponse> response = Observable.defer(new Func0<Observable<NoopResponse>>() {
            @Override
            public Observable<NoopResponse> call() {
                CouchbaseRequest r = new NoopRequest(bucket, hostname);
                request.set(r);
                return core.send(r);
            }
        }).timeout(timeout, timeUnit);
        return mapToServiceHealth(bucket, ServiceType.BINARY, response, request, timeout, timeUnit);
    }

    /**
     * Helper method to perform the proper health conversion.
     *
     * @param input the response input
     * @return the converted output.
     */
    private static Observable<PingServiceHealth> mapToServiceHealth(final String scope,
        final ServiceType type, final Observable<? extends CouchbaseResponse> input,
        final AtomicReference<CouchbaseRequest> request, final long timeout, final TimeUnit timeUnit) {
        return input
            .map(new Func1<CouchbaseResponse, PingServiceHealth>() {
                @Override
                public PingServiceHealth call(CouchbaseResponse response) {
                    DiagnosticRequest request = (DiagnosticRequest) response.request();
                    String id = "0x" + Integer.toHexString(request.localSocket().hashCode());
                    return new PingServiceHealth(
                        type,
                        PingServiceHealth.PingState.OK,
                        id,
                        TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - response.request().creationTime()),
                        request.localSocket(),
                        request.remoteSocket(),
                        scope
                    );
                }
            })
            .onErrorReturn(new Func1<Throwable, PingServiceHealth>() {
                @Override
                public PingServiceHealth call(Throwable throwable) {
                    SocketAddress local = ((DiagnosticRequest) request.get()).localSocket();
                    SocketAddress remote = ((DiagnosticRequest) request.get()).remoteSocket();
                    String id = local == null ? "0x0000" : "0x" + Integer.toHexString(local.hashCode());
                    if (throwable instanceof TimeoutException) {
                        return new PingServiceHealth(
                            type,
                            PingServiceHealth.PingState.TIMEOUT,
                            id,
                            timeUnit.toMicros(timeout),
                            local,
                            remote,
                            scope
                        );
                    } else {
                        LOGGER.warn("Error while running PingService for {}", type, throwable);
                        return new PingServiceHealth(
                            type,
                            PingServiceHealth.PingState.ERROR,
                            id,
                            TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - request.get().creationTime()),
                            local,
                            remote,
                            scope
                        );
                    }
                }
            });
    }

    /**
     * Pings the service and completes if successful - and fails if it didn't work
     * for some reason (reason is in the exception).
     */
    private static Observable<PingServiceHealth> pingQuery(
        final String hostname, final String bucket, final String password,
        final ClusterFacade core, final long timeout, final TimeUnit timeUnit) {
        final AtomicReference<CouchbaseRequest> request = new AtomicReference<CouchbaseRequest>();
        Observable<com.couchbase.client.core.message.query.PingResponse> response =
            Observable.defer(new Func0<Observable<com.couchbase.client.core.message.query.PingResponse>>() {
            @Override
            public Observable<com.couchbase.client.core.message.query.PingResponse> call() {
                CouchbaseRequest r;
                try {
                    r = new com.couchbase.client.core.message.query.PingRequest(hostname, bucket, password);
                } catch (Exception e) {
                    return Observable.error(e);
                }
                request.set(r);
                return core.send(r);
            }
        }).timeout(timeout, timeUnit);
        return mapToServiceHealth(null, ServiceType.QUERY, response, request, timeout, timeUnit);
    }

    /**
     * Pings the service and completes if successful - and fails if it didn't work
     * for some reason (reason is in the exception).
     */
    private static Observable<PingServiceHealth> pingSearch(
        final String hostname, final String bucket, final String password,
        final ClusterFacade core, final long timeout, final TimeUnit timeUnit) {
        final AtomicReference<CouchbaseRequest> request = new AtomicReference<CouchbaseRequest>();
        Observable<com.couchbase.client.core.message.search.PingResponse> response =
            Observable.defer(new Func0<Observable<com.couchbase.client.core.message.search.PingResponse>>() {
                @Override
                public Observable<com.couchbase.client.core.message.search.PingResponse> call() {
                    CouchbaseRequest r;
                    try {
                        r = new com.couchbase.client.core.message.search.PingRequest(hostname, bucket, password);
                    } catch (Exception e) {
                        return Observable.error(e);
                    }
                    request.set(r);
                    return core.send(r);
                }
            }).timeout(timeout, timeUnit);
        return mapToServiceHealth(null, ServiceType.SEARCH, response, request, timeout, timeUnit);
    }

    /**
     * Pings the service and completes if successful - and fails if it didn't work
     * for some reason (reason is in the exception).
     */
    private static Observable<PingServiceHealth> pingViews(
        final String hostname, final String bucket, final String password,
        final ClusterFacade core, final long timeout, final TimeUnit timeUnit) {
        final AtomicReference<CouchbaseRequest> request = new AtomicReference<CouchbaseRequest>();
        Observable<com.couchbase.client.core.message.view.PingResponse> response =
            Observable.defer(new Func0<Observable<com.couchbase.client.core.message.view.PingResponse>>() {
                @Override
                public Observable<com.couchbase.client.core.message.view.PingResponse> call() {
                    CouchbaseRequest r;
                    try {
                        r = new com.couchbase.client.core.message.view.PingRequest(hostname, bucket, password);
                    } catch (Exception e) {
                        return Observable.error(e);
                    }
                    request.set(r);
                    return core.send(r);
                }
            }).timeout(timeout, timeUnit);
        return mapToServiceHealth(null, ServiceType.VIEW, response, request, timeout, timeUnit);
    }

    /**
     * Pings the service and completes if successful - and fails if it didn't work
     * for some reason (reason is in the exception).
     */
    private static Observable<PingServiceHealth> pingAnalytics(
        final String hostname, final String bucket, final String password,
        final ClusterFacade core, final long timeout, final TimeUnit timeUnit) {
        final AtomicReference<CouchbaseRequest> request = new AtomicReference<CouchbaseRequest>();
        Observable<com.couchbase.client.core.message.analytics.PingResponse> response =
            Observable.defer(new Func0<Observable<com.couchbase.client.core.message.analytics.PingResponse>>() {
                @Override
                public Observable<com.couchbase.client.core.message.analytics.PingResponse> call() {
                    CouchbaseRequest r;
                    try {
                        r = new com.couchbase.client.core.message.analytics.PingRequest(hostname, bucket, password);
                    } catch (Exception e) {
                        return Observable.error(e);
                    }
                    request.set(r);
                    return core.send(r);
                }
            }).timeout(timeout, timeUnit);
        return mapToServiceHealth(null, ServiceType.ANALYTICS, response, request, timeout, timeUnit);
    }
}
