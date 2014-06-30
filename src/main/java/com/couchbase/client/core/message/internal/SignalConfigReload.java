package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class SignalConfigReload extends AbstractCouchbaseRequest {

    public static SignalConfigReload INSTANCE = new SignalConfigReload();

    private SignalConfigReload() {
        super(null, null);
    }
}
