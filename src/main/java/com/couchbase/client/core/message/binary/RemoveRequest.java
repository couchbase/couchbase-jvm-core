package com.couchbase.client.core.message.binary;

public class RemoveRequest extends AbstractBinaryRequest {

    private final long cas;

    public RemoveRequest(String key, String bucket) {
        this(key, 0, bucket);
    }

    public RemoveRequest(String key, long cas, String bucket) {
        super(key, bucket, null);
        this.cas = cas;
    }

    /**
     * The CAS value of the request.
     *
     * @return the cas value.
     */
    public long cas() {
        return cas;
    }

}
