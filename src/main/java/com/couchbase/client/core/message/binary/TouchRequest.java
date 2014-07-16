package com.couchbase.client.core.message.binary;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class TouchRequest extends AbstractBinaryRequest {

    private final int expiry;

    public TouchRequest(String key, int expiry, String bucket) {
        super(key, bucket, null);
        this.expiry = expiry;
    }

    public int expiry() {
        return expiry;
    }

}
