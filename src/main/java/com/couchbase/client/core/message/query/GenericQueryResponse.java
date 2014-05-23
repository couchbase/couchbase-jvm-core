package com.couchbase.client.core.message.query;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

/**
 * Created by michael on 21/05/14.
 */
public class GenericQueryResponse extends AbstractCouchbaseResponse {

    private final String content;

    public GenericQueryResponse(String content, ResponseStatus status, CouchbaseRequest request) {
        super(status, request);
        this.content = content;
    }

    public String content() {
        return content;
    }


}
