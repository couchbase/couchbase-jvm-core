package com.couchbase.client.core.message.view;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class GetDesignDocumentRequest extends AbstractCouchbaseRequest implements ViewRequest {

    private final String name;
    private final boolean development;

    public GetDesignDocumentRequest(String name, boolean development, String bucket, String password) {
        super(bucket, password);
        this.name = name;
        this.development = development;
    }

    public String name() {
        return name;
    }

    public boolean development() {
        return development;
    }
}
