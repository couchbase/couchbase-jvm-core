package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.lmax.disruptor.EventHandler;

public class ResponseHandler implements EventHandler<ResponseEvent> {

    @Override
    public void onEvent(final ResponseEvent event, long sequence, boolean endOfBatch) throws Exception {
        CouchbaseResponse response = event.getResponse();
        ResponseStatus status = response.status();
        if (status == ResponseStatus.CHUNKED || status == ResponseStatus.SUCCESS) {
            event.getObservable().onNext(response);
            if (status == ResponseStatus.SUCCESS) {
                event.getObservable().onCompleted();
            }
        } else {
            throw new IllegalStateException("fixme in response handler: " + event);
        }
    }
}
