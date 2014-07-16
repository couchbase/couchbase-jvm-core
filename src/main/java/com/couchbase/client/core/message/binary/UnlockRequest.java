package com.couchbase.client.core.message.binary;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class UnlockRequest extends AbstractBinaryRequest {

    private final long cas;

    public UnlockRequest(String key, long cas, String bucket) {
        super(key, bucket, null);
        this.cas = cas;
    }

    public long cas() {
        return cas;
    }

}
