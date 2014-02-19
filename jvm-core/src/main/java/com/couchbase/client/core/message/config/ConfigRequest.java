package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.service.ServiceType;

public interface ConfigRequest extends CouchbaseRequest {

    ServiceType type();
    String bucket();

}
