package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

/**
 * Created by michael on 22/05/14.
 */
public class VerboseBucketConfigRequest extends AbstractCouchbaseRequest implements ConfigRequest {

    private static final String PATH = "/pools/default/buckets/";

    private final String hostname;

    public VerboseBucketConfigRequest(String hostname, String bucket, String password) {
        super(bucket, password);
        this.hostname = hostname;
    }

    public String hostname() {
        return hostname;
    }

    public String path() {
        return PATH + bucket();
    }
}
