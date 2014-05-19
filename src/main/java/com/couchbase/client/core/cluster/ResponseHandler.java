package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorTwoArg;
import rx.subjects.Subject;

public class ResponseHandler implements EventHandler<ResponseEvent> {

    private final Cluster cluster;

    public ResponseHandler(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Translates {@link CouchbaseRequest}s into {@link RequestEvent}s.
     */
    public static final EventTranslatorTwoArg<ResponseEvent, CouchbaseMessage, Subject<CouchbaseResponse, CouchbaseResponse>> RESPONSE_TRANSLATOR =
        new EventTranslatorTwoArg<ResponseEvent, CouchbaseMessage, Subject<CouchbaseResponse, CouchbaseResponse>>() {
            @Override
            public void translateTo(ResponseEvent event, long sequence, CouchbaseMessage message, Subject<CouchbaseResponse, CouchbaseResponse> observable) {
                event.setMessage(message);
                event.setObservable(observable);
            }
        };

    @Override
    public void onEvent(final ResponseEvent event, long sequence, boolean endOfBatch) throws Exception {
        CouchbaseMessage message = event.getMessage();
        if (message instanceof CouchbaseResponse) {
            CouchbaseResponse response = (CouchbaseResponse) message;
            ResponseStatus status = response.status();
            switch(status) {
                case CHUNKED:
                    event.getObservable().onNext(response);
                    break;
                case SUCCESS:
                case EXISTS:
                case NOT_EXISTS:
                case FAILURE:
                    event.getObservable().onNext(response);
                    event.getObservable().onCompleted();
                    break;
                default:
                    throw new UnsupportedOperationException("fixme");
            }
        } else if (message instanceof CouchbaseRequest) {
            cluster.send((CouchbaseRequest) message);
        } else {
            throw new IllegalStateException("Got message type I do not understand: " + message);
        }
    }
}
