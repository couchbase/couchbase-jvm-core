package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.message.CouchbaseResponse;

public class SeedNodesResponse implements CouchbaseResponse {

    private final boolean success;

    public SeedNodesResponse(boolean success) {
        this.success = success;
    }

    public boolean success() {
        return success;
    }
}
