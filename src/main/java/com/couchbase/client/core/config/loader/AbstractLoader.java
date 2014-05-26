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

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.cluster.Cluster;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.internal.AddNodeRequest;
import com.couchbase.client.core.message.internal.AddNodeResponse;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.AddServiceResponse;
import com.couchbase.client.core.service.ServiceType;
import com.fasterxml.jackson.databind.ObjectMapper;
import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

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
     * Jackson object mapper for JSON parsing.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * The reference to the cluster.
     */
    private final Cluster cluster;

    /**
     * The couchbase environment.
     */
    private final Environment environment;

    /**
     * The service type from the actual loader implementation.
     */
    private final ServiceType type;

    /**
     * Create a new {@link AbstractLoader}.
     *
     * @param type the service type.
     * @param cluster the cluster reference.
     * @param environment the couchbase environment.
     */
    protected AbstractLoader(final ServiceType type, final Cluster cluster, final Environment environment) {
        this.type = type;
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
    protected abstract Observable<String> discoverConfig(String bucket, String password, String hostname);

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
    public Observable<BucketConfig> loadConfig(final Set<InetAddress> seedNodes, final String bucket,
        final String password) {
        return Observable
            .from(seedNodes, Schedulers.computation())
            .flatMap(new Func1<InetAddress, Observable<AddNodeResponse>>() {
                @Override
                public Observable<AddNodeResponse> call(final InetAddress address) {
                    return cluster.send(new AddNodeRequest(address.getHostName()));
                }
            }).flatMap(new Func1<AddNodeResponse, Observable<AddServiceResponse>>() {
                @Override
                public Observable<AddServiceResponse> call(final AddNodeResponse response) {
                    if (!response.status().isSuccess()) {
                        return Observable.error(new IllegalStateException("Could not add node for config loading."));
                    }
                    return cluster.send(
                            new AddServiceRequest(type, bucket, password, port(), response.hostname())
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
            }).map(new Func1<String, BucketConfig>() {
                @Override
                public BucketConfig call(final String rawConfig) {
                    try {
                        BucketConfig config = OBJECT_MAPPER.readValue(rawConfig, BucketConfig.class);
                        config.password(password);
                        return config;
                    } catch (Exception ex) {
                        throw new CouchbaseException("Could not parse configuration", ex);
                    }
                }
            });
    }

    /**
     * Returns the {@link Cluster} for child implementations.
     *
     * @return the cluster reference.
     */
    protected Cluster cluster() {
        return cluster;
    }

    /**
     * Returns the {@link Environment} for child implementations.
     *
     * @return the environment.
     */
    protected Environment env() {
        return environment;
    }

    /**
     * Replaces the host wildcard from an incoming config with a proper hostname.
     *
     * @param input the input config.
     * @param hostname the hostname to replace it with.
     * @return a replaced configuration.
     */
    protected String replaceHostWildcard(String input, String hostname) {
        return input.replace("$HOST", hostname);
    }
}
