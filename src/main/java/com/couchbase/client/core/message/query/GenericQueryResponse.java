package com.couchbase.client.core.message.query;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;
import rx.Observable;


public class GenericQueryResponse extends AbstractCouchbaseResponse {

    private final Observable<ByteBuf> rows;
    private final Observable<ByteBuf> info;

    public GenericQueryResponse(Observable<ByteBuf> rows, Observable<ByteBuf> info, ResponseStatus status,
        CouchbaseRequest request) {
        super(status, request);
        this.rows = rows;
        this.info = info;
    }

    public Observable<ByteBuf> rows() {
        return rows;
    }
    public Observable<ByteBuf> info() { return info; }

}
