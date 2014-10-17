package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.BootstrapMessage;

public class BucketsConfigRequest extends AbstractCouchbaseRequest implements ConfigRequest, BootstrapMessage {

    public BucketsConfigRequest(String bucket, String password) {
        super(bucket, password);
    }

    @Override
    public String path() {
        return "/pools/default/buckets/";
    }

}
