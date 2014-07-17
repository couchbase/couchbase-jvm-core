package com.couchbase.client.core.message.binary;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class ObserveRequest extends AbstractBinaryRequest {

    private final long cas;
    private final boolean master;
    private final short replica;

    public ObserveRequest(String key, long cas, boolean master, short replica, String bucket) {
        super(key, bucket, null);
        if (master && replica > 0) {
            throw new IllegalArgumentException("Either master or a replica node needs to be given");
        }
        this.cas = cas;
        this.master = master;
        this.replica = replica;
    }

    public long cas() {
        return cas;
    }

    public short replica() {
        return replica;
    }

    public boolean master() {
        return master;
    }
}
