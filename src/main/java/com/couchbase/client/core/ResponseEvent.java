package com.couchbase.client.core;

import com.couchbase.client.core.message.CouchbaseMessage;
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
     * Contains the current response message.
     */
    private CouchbaseMessage message;

    private Subject<CouchbaseResponse, CouchbaseResponse> observable;

    /**
     * Set the new response as a payload for this event.
     *
     * @param message the response to override.
     * @return the {@link ResponseEvent} for method chaining.
     */
    public ResponseEvent setMessage(final CouchbaseMessage message) {
        this.message = message;
        return this;
    }

    /**
     * Get the response from the payload.
     *
     * @return the actual response.
     */
    public CouchbaseMessage getMessage() {
        return message;
    }

    public Subject<CouchbaseResponse, CouchbaseResponse> getObservable() {
        return observable;
    }

    public ResponseEvent setObservable(final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        this.observable = observable;
        return this;
    }
}
