package com.couchbase.client.core.message.view;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class ViewQueryRequest extends AbstractCouchbaseRequest implements ViewRequest {

    private final String design;
    private final String view;
    private final String query;

    public ViewQueryRequest(String design, String view, String bucket, String password) {
        this(design, view, null, bucket, password);
    }

    public ViewQueryRequest(String design, String view, String query, String bucket, String password) {
        super(bucket, password);
        this.design = design;
        this.view = view;
        this.query = query;
    }

    public String design() {
        return design;
    }

    public String view() {
        return view;
    }

    public String query() {
        return query;
    }
}
