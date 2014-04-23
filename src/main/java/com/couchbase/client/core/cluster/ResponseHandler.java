package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.CouchbaseResponse;
import com.lmax.disruptor.EventHandler;

public class ResponseHandler implements EventHandler<ResponseEvent> {

    @Override
    public void onEvent(final ResponseEvent event, long sequence, boolean endOfBatch) throws Exception {
        CouchbaseResponse response = event.getResponse();
        event.getObservable().onNext(response);
        event.getObservable().onCompleted();
    }
}
