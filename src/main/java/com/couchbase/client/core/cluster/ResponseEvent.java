package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.CouchbaseResponse;
import rx.subjects.Subject;

/**
 * A pre allocated event which carries a {@link CouchbaseResponse} and associated information.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ResponseEvent {

    /**
     * Contains the current request.
     */
    private CouchbaseResponse response;

    private Subject<CouchbaseResponse, CouchbaseResponse> observable;

    /**
     * Set the new response as a payload for this event.
     *
     * @param response the response to override.
     * @return the {@link ResponseEvent} for method chaining.
     */
    public ResponseEvent setResponse(final CouchbaseResponse response) {
        this.response = response;
        return this;
    }

    /**
     * Get the response from the payload.
     *
     * @return the actual response.
     */
    public CouchbaseResponse getResponse() {
        return response;
    }

    public Subject<CouchbaseResponse, CouchbaseResponse> getObservable() {
        return observable;
    }

    public ResponseEvent setObservable(final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        this.observable = observable;
        return this;
    }
}
