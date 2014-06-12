package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

/**
 * Sends a flush command to the cluster.
 *
 * Note that depending on the return value, the executing client needs to perform more tasks like adding a document
 * and polling for its removal.
 *
 * @author Michael Nitschinger
 */
public class FlushRequest extends AbstractCouchbaseRequest implements ConfigRequest {

    private static final String PATH = "/controller/doFlush";

    public FlushRequest(String bucket, String password) {
        super(bucket, password);
    }

    public String path() {
        return "/pools/default/buckets/" + bucket() + PATH;
    }
}
