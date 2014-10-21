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
package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.LoaderType;
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.internal.AddNodeRequest;
import com.couchbase.client.core.message.internal.AddNodeResponse;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.AddServiceResponse;
import com.couchbase.client.core.service.ServiceType;
import rx.Observable;
import rx.functions.Func1;

import java.net.InetAddress;
import java.util.Set;

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
    protected AbstractLoader(final LoaderType loaderType, final ServiceType serviceType, final ClusterFacade cluster, final CoreEnvironment environment) {
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
     * @param password the password of the bucket.
     * @param hostname the hostname of the seed node list.
     * @return a raw config if discovered.
     */
    protected abstract Observable<String> discoverConfig(String bucket, String password, InetAddress hostname);

    /**
     * Initiate the config loading process.
     *
     * The common path handled by this abstract implementation includes making sure that the node and service are
     * usable by the actual implementation. Finally, the raw config string config parsing is handled in this central
     * place.
     *
     * @param seedNodes the seed nodes.
     * @param bucket the name of the bucket.
     * @param password the password of the bucket.
     * @return a valid {@link BucketConfig}.
     */
    public Observable<Tuple2<LoaderType, BucketConfig>> loadConfig(final Set<InetAddress> seedNodes, final String bucket,
        final String password) {
        return Observable
            .from(seedNodes)
            .subscribeOn(env().scheduler())
            .flatMap(new Func1<InetAddress, Observable<AddNodeResponse>>() {
                @Override
                public Observable<AddNodeResponse> call(final InetAddress address) {
                    return cluster.send(new AddNodeRequest(address));
                }
            }).flatMap(new Func1<AddNodeResponse, Observable<AddServiceResponse>>() {
                @Override
                public Observable<AddServiceResponse> call(final AddNodeResponse response) {
                    if (!response.status().isSuccess()) {
                        return Observable.error(new IllegalStateException("Could not add node for config loading."));
                    }
                    return cluster.send(
                            new AddServiceRequest(serviceType, bucket, password, port(), response.hostname())
                    );
                }
            }).flatMap(new Func1<AddServiceResponse, Observable<String>>() {
                @Override
                public Observable<String> call(final AddServiceResponse response) {
                    if (!response.status().isSuccess()) {
                        return Observable.error(new IllegalStateException("Could not add service for config loading."));
                    }

                    return discoverConfig(bucket, password, response.hostname());
                }
            })
            .map(new Func1<String, Tuple2<LoaderType, BucketConfig>>() {
                @Override
                public Tuple2<LoaderType, BucketConfig> call(final String rawConfig) {
                    BucketConfig config = BucketConfigParser.parse(rawConfig);
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
    protected String replaceHostWildcard(String input, InetAddress hostname) {
        return input.replace("$HOST", hostname.getHostName());
    }
}
