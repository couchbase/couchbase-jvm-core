package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.config.BucketConfig;
import rx.Observable;

import java.net.InetAddress;
import java.util.Set;

/**
 * Created by michael on 26/05/14.
 */
public interface Loader {

    public Observable<BucketConfig> loadConfig(final Set<InetAddress> seedNodes, final String bucket,
                                               final String password);
}
