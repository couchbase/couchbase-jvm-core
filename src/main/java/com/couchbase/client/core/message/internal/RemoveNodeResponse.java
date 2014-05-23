package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

/**
 * Created by michael on 20/05/14.
 */
public class RemoveNodeResponse extends AbstractCouchbaseResponse {

    public RemoveNodeResponse(ResponseStatus status) {
        super(status, null);
    }
}
