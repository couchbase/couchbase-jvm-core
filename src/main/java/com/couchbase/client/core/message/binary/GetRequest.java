package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

/**
 * Fetch a document from the cluster and return it if found.
 */
public class GetRequest extends AbstractCouchbaseRequest implements BinaryRequest {

    /**
     * The key of the document
     */
    private final String key;

    /**
     * Create a new {@link GetRequest}.
     *
     * @param key the key of the document.
     */
    public GetRequest(final String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public short partition() {
        return 0;
    }

    @Override
    public BinaryRequest partition(short id) {
        return null;
    }
}
