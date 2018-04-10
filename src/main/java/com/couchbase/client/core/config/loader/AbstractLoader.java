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
package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.LoaderType;
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.internal.AddNodeRequest;
import com.couchbase.client.core.message.internal.AddNodeResponse;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.AddServiceResponse;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.message.internal.RemoveServiceResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.utils.NetworkAddress;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.net.InetAddress;

/**
 * An {@link AbstractLoader} which provides common basic processing for all implementing loaders.
 *
 * A loader fetches configuration from a service, maybe falls back to another service and finally response with a
 * {@link BucketConfig} or an error. There are multiple steps, like making sure that a node or service is alive before
 * sending a request into, is abstracted in here to avoid duplication.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public abstract class AbstractLoader implements Loader {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Loader.class);

    /**
     * The reference to the cluster.
     */
    private final ClusterFacade cluster;

    /**
     * The couchbase environment.
     */
    private final CoreEnvironment environment;

    /**
     * The service serviceType from the actual loader implementation.
     */
    private final ServiceType serviceType;

    private final LoaderType loaderType;

    /**
     * Create a new {@link AbstractLoader}.
     *
     * @param serviceType the service serviceType.
     * @param cluster the cluster reference.
     * @param environment the couchbase environment.
     */
    protected AbstractLoader(final LoaderType loaderType, final ServiceType serviceType, final ClusterFacade cluster,
        final CoreEnvironment environment) {
        this.loaderType = loaderType;
        this.serviceType = serviceType;
        this.cluster = cluster;
        this.environment = environment;
    }

    /**
     * Port to use for the {@link ServiceType}.
     *
     * This method needs to be implemented by the actual loader and defines the port which should be used to
     * connect the service to. In practice, the actual port may depend on the environment (i.e. if SSL is used or not).
     *
     * @return the port for the service to enable.
     */
    protected abstract int port();

    /**
     * Run the {@link BucketConfig} discovery process.
     *
     * @param bucket the name of the bucket.
     * @param username user authorized for bucket access.
     * @param password the password of the user.
     * @param hostname the hostname of the seed node list.
     * @return a raw config if discovered.
     */
    protected abstract Observable<String> discoverConfig(String bucket, String username, String password, NetworkAddress hostname);

    /**
     * Initiate the config loading process.
     *
     * @param seedNode the seed node.
     * @param bucket the name of the bucket.
     * @param password the password of the bucket.
     * @return a valid {@link BucketConfig}.
     */
    public Observable<Tuple2<LoaderType, BucketConfig>> loadConfig(final NetworkAddress seedNode, final String bucket,
                                                                   final String password) {
        LOGGER.debug("Loading Config for bucket {}", bucket);
        return loadConfig(seedNode, bucket, bucket, password);
    }

    /**
     * Initiate the config loading process.
     *
     * @param seedNode the seed node.
     * @param bucket the name of the bucket.
     * @param username the user authorized for bucket access.
     * @param password the password of the user.
     * @return a valid {@link BucketConfig}.
     */
    public Observable<Tuple2<LoaderType, BucketConfig>> loadConfig(final NetworkAddress seedNode, final String bucket,
                                                                   final String username, final String password) {
        LOGGER.debug("Loading Config for bucket {}", bucket);
        return loadConfigAtAddr(seedNode, bucket, username, password);
    }

    /**
     * Helper method to load a config at a specific {@link InetAddress}.
     *
     * The common path handled by this abstract implementation includes making sure that the node and service are
     * usable by the actual implementation. Finally, the raw config string config parsing is handled in this central
     * place.
     *
     * @param node the node to grab a config from.
     * @param bucket the name of the bucket.
     * @param username the user authorized for bucket access.
     * @param password the password of the user.
     * @return a valid {@link BucketConfig} or an errored {@link Observable}.
     */
    private Observable<Tuple2<LoaderType, BucketConfig>> loadConfigAtAddr(final NetworkAddress node,
                                                                          final String bucket,
                                                                          final String username,
                                                                          final String password) {
        return Observable
            .just(node)
            .flatMap(new Func1<NetworkAddress, Observable<AddNodeResponse>>() {
                @Override
                public Observable<AddNodeResponse> call(final NetworkAddress address) {
                    return cluster.send(new AddNodeRequest(address));
                }
            }).flatMap(new Func1<AddNodeResponse, Observable<AddServiceResponse>>() {
                @Override
                public Observable<AddServiceResponse> call(final AddNodeResponse response) {
                    if (!response.status().isSuccess()) {
                        return Observable.error(new IllegalStateException("Could not add node for config loading."));
                    }
                    LOGGER.debug("Successfully added Node {}", response.hostname());
                    return cluster.<AddServiceResponse>send(
                        new AddServiceRequest(serviceType, bucket, username, password, port(), response.hostname())
                    ).onErrorResumeNext(new Func1<Throwable, Observable<AddServiceResponse>>() {
                        @Override
                        public Observable<AddServiceResponse> call(Throwable throwable) {
                            LOGGER.debug("Could not add service on {} because of {}, removing it again.",
                                node, throwable);
                            return cluster.<RemoveServiceResponse>send(new RemoveServiceRequest(serviceType, bucket, node))
                                .map(new Func1<RemoveServiceResponse, AddServiceResponse>() {
                                    @Override
                                    public AddServiceResponse call(RemoveServiceResponse removeServiceResponse) {
                                        return new AddServiceResponse(ResponseStatus.FAILURE, response.hostname());
                                    }
                                });
                        }
                    });
                }
            }).flatMap(new Func1<AddServiceResponse, Observable<String>>() {
                @Override
                public Observable<String> call(final AddServiceResponse response) {
                    if (!response.status().isSuccess()) {
                        return Observable.error(new IllegalStateException("Could not add service for config loading."));
                    }
                    LOGGER.debug("Successfully enabled Service {} on Node {}", serviceType, response.hostname());
                    return discoverConfig(bucket, username, password, response.hostname());
                }
            })
            .map(new Func1<String, Tuple2<LoaderType, BucketConfig>>() {
                @Override
                public Tuple2<LoaderType, BucketConfig> call(final String rawConfig) {
                    LOGGER.debug("Got configuration from Service, attempting to parse.");
                    BucketConfig config = BucketConfigParser.parse(rawConfig, env(), node);
                    config.username(username);
                    config.password(password);
                    return Tuple.create(loaderType, config);
                }
            });
    }

    /**
     * Returns the {@link ClusterFacade} for child implementations.
     *
     * @return the cluster reference.
     */
    protected ClusterFacade cluster() {
        return cluster;
    }

    /**
     * Returns the {@link CoreEnvironment} for child implementations.
     *
     * @return the environment.
     */
    protected CoreEnvironment env() {
        return environment;
    }

    /**
     * Replaces the host wildcard from an incoming config with a proper hostname.
     *
     * @param input the input config.
     * @param hostname the hostname to replace it with.
     * @return a replaced configuration.
     */
    protected String replaceHostWildcard(String input, NetworkAddress hostname) {
        return input.replace("$HOST", hostname.address());
    }
}
