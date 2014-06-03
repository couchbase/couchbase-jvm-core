package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

import java.net.InetAddress;

public class BucketConfigRequest extends AbstractCouchbaseRequest implements ConfigRequest {

    private static final String PATH = "/pools/default/b/";

    private final InetAddress hostname;
    private final String path;

    public BucketConfigRequest(String path, InetAddress hostname, String bucket, String password) {
        super(bucket, password);
        this.hostname = hostname;
        this.path = path;
    }

    public InetAddress hostname() {
        return hostname;
    }

    public String path() {
        return path + bucket();
    }
}
