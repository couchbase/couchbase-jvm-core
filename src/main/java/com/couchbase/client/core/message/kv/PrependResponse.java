package com.couchbase.client.core.message.kv;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class PrependResponse extends AbstractKeyValueResponse {

    private final long cas;
    private final MutationToken mutationToken;

    public PrependResponse(ResponseStatus status, short serverStatusCode, long cas, String bucket, ByteBuf content,
        MutationToken mutationToken, CouchbaseRequest request) {
        super(status, serverStatusCode, bucket, content, request);
        this.cas = cas;
        this.mutationToken = mutationToken;
    }

    public long cas() {
        return cas;
    }

    public MutationToken mutationToken() {
        return mutationToken;
    }

}
