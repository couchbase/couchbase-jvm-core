package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.LoaderType;
import com.couchbase.client.core.lang.Tuple2;
import rx.Observable;

import java.net.InetAddress;
import java.util.Set;

public interface Loader {

    public Observable<Tuple2<LoaderType, BucketConfig>> loadConfig(final Set<InetAddress> seedNodes,
        final String bucket, final String password);
}
