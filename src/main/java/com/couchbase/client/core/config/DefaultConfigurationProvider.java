package com.couchbase.client.core.config;

import com.couchbase.client.core.cluster.Cluster;
import rx.Observable;

public class DefaultConfigurationProvider implements ConfigurationProvider {

    private final Cluster cluster;

    public DefaultConfigurationProvider(final Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Observable<Configuration> configs() {
        return null;
    }
}
