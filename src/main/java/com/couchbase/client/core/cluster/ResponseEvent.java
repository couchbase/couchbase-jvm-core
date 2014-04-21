package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.CouchbaseResponse;

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

    public CouchbaseResponse getResponse() {
        return response;
    }
}
