package com.couchbase.client.core.message.view;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import rx.Observable;

public class ViewQueryResponse extends AbstractCouchbaseResponse {

    private final Observable<ByteBuf> rows;
    private final Observable<ByteBuf> info;


    public ViewQueryResponse(Observable<ByteBuf> rows, Observable<ByteBuf> info, ResponseStatus status,
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
