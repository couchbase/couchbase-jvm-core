package com.couchbase.client.core.message.kv;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class TouchResponse extends AbstractKeyValueResponse {

    public TouchResponse(ResponseStatus status, short serverStatusCode, String bucket, ByteBuf content,
                         CouchbaseRequest request) {
        super(status, serverStatusCode, bucket, content, request);
    }
}
