package com.couchbase.client.core;

import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.binary.BinaryResponse;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorTwoArg;
import io.netty.util.CharsetUtil;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.schedulers.Schedulers;
import rx.subjects.Subject;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ResponseHandler implements EventHandler<ResponseEvent> {

    private final ClusterFacade cluster;
    private final ConfigurationProvider configurationProvider;
    private final Scheduler.Worker worker;


    public ResponseHandler(ClusterFacade cluster, ConfigurationProvider provider) {
        this.cluster = cluster;
        this.configurationProvider = provider;
        this.worker = Schedulers.computation().createWorker();
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

    /**
     * Handles {@link ResponseEvent}s that come into the response RingBuffer.
     *
     * Hey I just mapped you,
     * And this is crazy,
     * But here's my data
     * so subscribe me maybe.
     *
     * It's hard to block right,
     * at you baby,
     * But here's my data ,
     * so subscribe me maybe.
     */
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
                case RETRY:
                    retry(event);
                    break;
                default:
                    throw new UnsupportedOperationException("fixme");
            }
        } else if (message instanceof CouchbaseRequest) {
            retry(event);
        } else {
            throw new IllegalStateException("Got message type I do not understand: " + message);
        }
    }

    private void retry(final ResponseEvent event) {
        final CouchbaseMessage message = event.getMessage();
        if (message instanceof CouchbaseRequest) {
            scheduleForRetry((CouchbaseRequest) message);
        } else {
            CouchbaseRequest request = ((CouchbaseResponse) message).request();
            if (request != null) {
                scheduleForRetry(request);
            } else {
                event.getObservable().onError(new CouchbaseException("Operation failed because it does not " +
                    "support cloning."));
            }
            if (message instanceof BinaryResponse) {
                BinaryResponse response = (BinaryResponse) message;
                if (response.content() != null && response.content().readableBytes() > 0) {
                    configurationProvider.proposeBucketConfig(response.bucket(),
                        response.content().toString(CharsetUtil.UTF_8));
                }
            }
        }
    }

    private void scheduleForRetry(final CouchbaseRequest request) {
        final AtomicReference<Subscription> subscription = new AtomicReference<Subscription>();
        subscription.set(worker.schedule(new Action0() {
            @Override
            public void call() {
                cluster.send(request);
                subscription.get().unsubscribe();
            }
        }, 10, TimeUnit.MILLISECONDS));
    }
}
