package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

/**
 * Disconnects all open buckets.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class DisconnectRequest extends AbstractCouchbaseRequest implements ClusterRequest {
    public DisconnectRequest() {
        super(null, null);
    }
}
