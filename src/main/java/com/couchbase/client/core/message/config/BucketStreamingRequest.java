package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class BucketStreamingRequest extends AbstractCouchbaseRequest implements ConfigRequest {

    private final String path;

    public BucketStreamingRequest(final String path, final String bucket, final String password) {
        super(bucket, password);
        this.path = path;
    }

    public String path() {
        return path + bucket();
    }
}
