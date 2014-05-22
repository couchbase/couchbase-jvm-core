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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractLoader {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Cluster cluster;
    private final Environment environment;
    private final AtomicReference<List<InetAddress>> seedNodes;

    protected AbstractLoader(Cluster cluster, Environment environment, AtomicReference<List<InetAddress>> seedNodes) {
        this.cluster = cluster;
        this.environment = environment;
        this.seedNodes = seedNodes;
    }

    protected abstract ServiceType serviceType();
    protected abstract int port();
    protected abstract Observable<String> discoverConfig(String bucket, String password, String hostname);

    public Observable<BucketConfig> loadConfig(final String bucket, final String password) {
        return Observable
            .from(seedNodes.get(), Schedulers.computation())
            .flatMap(new Func1<InetAddress, Observable<AddNodeResponse>>() {
                @Override
                public Observable<AddNodeResponse> call(InetAddress addr) {
                    return cluster().send(new AddNodeRequest(addr.getHostName()));
                }
            }).flatMap(new Func1<AddNodeResponse, Observable<AddServiceResponse>>() {
                @Override
                public Observable<AddServiceResponse> call(AddNodeResponse response) {
                    AddServiceRequest request = new AddServiceRequest(serviceType(), bucket, password, port(),
                        response.hostname());
                    return cluster().send(request);
                }
            }).flatMap(new Func1<AddServiceResponse, Observable<String>>() {
                @Override
                public Observable<String> call(AddServiceResponse response) {
                    return discoverConfig(bucket, password, response.hostname());
                }
            }).map(new Func1<String, BucketConfig>() {
                @Override
                public BucketConfig call(String rawConfig) {
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

    protected Cluster cluster() {
        return cluster;
    }

    protected Environment environment() {
        return environment;
    }

}
