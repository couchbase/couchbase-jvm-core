package com.couchbase.client.core.config;

import rx.Observable;

public interface ConfigurationProvider {

    /**
     * Returns an {@link Observable}, which pushes a new {@link ClusterConfig} once available.
     *
     * @return the configuration.
     */
    Observable<ClusterConfig> configs();
}
