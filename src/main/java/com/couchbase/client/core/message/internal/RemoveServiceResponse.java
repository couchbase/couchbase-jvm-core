package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

/**
 * Created by michael on 20/05/14.
 */
public class RemoveServiceResponse extends AbstractCouchbaseResponse {

    public RemoveServiceResponse(ResponseStatus status) {
        super(status, null);
    }
}
