package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class CounterResponse extends AbstractBinaryResponse {

    private final long value;
    private final long cas;

    public CounterResponse(ResponseStatus status, String bucket, long value, long cas, CouchbaseRequest request) {
        super(status, bucket, null, request);
        this.value = value;
        this.cas = cas;
    }

    public long value() {
        return value;
    }

    public long cas() {
        return cas;
    }
}
