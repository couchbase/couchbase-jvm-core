package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class TouchResponse extends AbstractBinaryResponse {

    public TouchResponse(ResponseStatus status, String bucket, ByteBuf content, CouchbaseRequest request) {
        super(status, bucket, content, request);
    }
}
