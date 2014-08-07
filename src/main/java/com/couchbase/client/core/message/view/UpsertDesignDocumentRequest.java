package com.couchbase.client.core.message.view;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class UpsertDesignDocumentRequest extends AbstractCouchbaseRequest implements ViewRequest {

    private final String name;
    private final boolean development;
    private final String body;

    public UpsertDesignDocumentRequest(String name, String body, boolean development, String bucket, String password) {
        super(bucket, password);
        this.name = name;
        this.development = development;
        this.body = body;
    }

    public String name() {
        return name;
    }

    public boolean development() {
        return development;
    }

    public String body() {
        return body;
    }
}
