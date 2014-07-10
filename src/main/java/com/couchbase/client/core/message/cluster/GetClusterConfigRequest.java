package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class GetClusterConfigRequest extends AbstractCouchbaseRequest implements ClusterRequest {

    public GetClusterConfigRequest() {
        super(null, null);
    }

}
