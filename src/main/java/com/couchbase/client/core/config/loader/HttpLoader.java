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

import com.couchbase.client.core.cluster.Cluster;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.BucketConfigResponse;
import com.couchbase.client.core.message.config.TerseBucketConfigRequest;
import com.couchbase.client.core.message.config.VerboseBucketConfigRequest;
import com.couchbase.client.core.service.ServiceType;
import rx.Observable;
import rx.functions.Func1;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class HttpLoader extends AbstractLoader{


    public HttpLoader(Cluster cluster, Environment environment, AtomicReference<List<InetAddress>> seedNodes) {
        super(cluster, environment, seedNodes);
    }

    @Override
    protected ServiceType serviceType() {
        return ServiceType.CONFIG;
    }

    @Override
    protected int port() {
        Environment env = environment();
        return env.sslEnabled() ? env.bootstrapHttpSslPort() : env.bootstrapHttpDirectPort();
    }

    @Override
    protected Observable<String> discoverConfig(final String bucket, final String password, final String hostname) {
        return cluster()
            .<BucketConfigResponse>send(new TerseBucketConfigRequest(hostname, bucket, password))
            .flatMap(new Func1<BucketConfigResponse, Observable<BucketConfigResponse>>() {
                @Override
                public Observable<BucketConfigResponse> call(BucketConfigResponse response) {
                    if (response.status() == ResponseStatus.SUCCESS) {
                        return Observable.just(response);
                    }
                    return cluster().send(new VerboseBucketConfigRequest(hostname, bucket, password));
                }
            }).map(new Func1<BucketConfigResponse, String>() {
                @Override
                public String call(BucketConfigResponse response) {
                    return response.config().replace("$HOST", hostname);
                }
            });
    }
}
