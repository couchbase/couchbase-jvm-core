package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

public class GetClusterConfigResponse extends AbstractCouchbaseResponse {

    private final ClusterConfig config;

    public GetClusterConfigResponse(ClusterConfig config, ResponseStatus status) {
        super(status, null);
        this.config = config;
    }

    public ClusterConfig config() {
        return config;
    }
}
