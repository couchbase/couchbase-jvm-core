package com.couchbase.client.core.service;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.config.GetBucketConfigResponse;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;

public class ConfigService implements Service {

    @Override
    public Promise<Boolean> shutdown() {
        return null;
    }

    @Override
    public  Promise<? extends CouchbaseResponse> send(CouchbaseRequest request) {
        return Promises.success(new GetBucketConfigResponse("foobar")).get();
    }
}
