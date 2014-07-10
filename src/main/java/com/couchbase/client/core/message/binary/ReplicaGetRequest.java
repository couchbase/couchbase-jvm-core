package com.couchbase.client.core.message.binary;

/**
 * Fetch a document from one or more and/or active nodes replicas.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ReplicaGetRequest extends AbstractBinaryRequest {

    private final short replica;

    public ReplicaGetRequest(String key, String bucket, short replica) {
        super(key, bucket, null);
        this.replica = replica;
    }

    public short replica() {
        return replica;
    }
}
