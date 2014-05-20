package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class DisconnectRequest extends AbstractCouchbaseRequest implements ClusterRequest {
    public DisconnectRequest() {
        super(null, null);
    }
}
